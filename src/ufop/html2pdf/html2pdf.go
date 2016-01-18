package html2pdf

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qiniu/log"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
	"ufop"
	"ufop/utils"
)

const (
	HTML2PDF_MAX_PAGE_SIZE = 10 * 1024 * 1024
	HTML2PDF_MAX_COPIES    = 10
)

type Html2Pdfer struct {
	maxPageSize uint64
	maxCopies   int
}

type Html2PdferConfig struct {
	Html2PdfMaxPageSize uint64 `json:"html2pdf_max_page_size,omitempty"`
	Html2PdfMaxCopies   int    `json:"html2pdf_max_copies,omitempty"`
}

type Html2PdfOptions struct {
	Gray        bool
	LowQuality  bool
	Orientation string
	Size        string
	Title       string
	Collate     bool
	Copies      int
}

func (this *Html2Pdfer) Name() string {
	return "html2pdf"
}

func (this *Html2Pdfer) InitConfig(jobConf string) (err error) {
	confFp, openErr := os.Open(jobConf)
	if openErr != nil {
		err = errors.New(fmt.Sprintf("Open html2pdf config failed, %s", openErr.Error()))
		return
	}

	config := Html2PdferConfig{}
	decoder := json.NewDecoder(confFp)
	decodeErr := decoder.Decode(&config)
	if decodeErr != nil {
		err = errors.New(fmt.Sprintf("Parse html2pdf config failed, %s", decodeErr.Error()))
		return
	}

	if config.Html2PdfMaxPageSize <= 0 {
		this.maxPageSize = HTML2PDF_MAX_PAGE_SIZE
	} else {
		this.maxPageSize = config.Html2PdfMaxPageSize
	}

	if config.Html2PdfMaxCopies <= 0 {
		this.maxCopies = HTML2PDF_MAX_COPIES
	} else {
		this.maxCopies = config.Html2PdfMaxCopies
	}

	return
}

func (this *Html2Pdfer) parse(cmd string) (url string, options *Html2PdfOptions, err error) {
	pattern := `^html2pdf/url/[0-9a-zA-Z-_=]+(/gray/[0|1]|/low/[0|1]|/orient/(Portrait|Landscape)|/size/[A-B][0-8]|/title/[0-9a-zA-Z-_=]+|/collate/[0|1]|/copies/\d+){0,7}$`
	matched, _ := regexp.MatchString(pattern, cmd)
	if !matched {
		err = errors.New("invalid html2pdf command format")
		return
	}

	var decodeErr error

	//url
	url, decodeErr = utils.GetParamDecoded(cmd, `url/[0-9a-zA-Z-_=]+`, "url")
	if decodeErr != nil {
		err = errors.New("invalid html2pdf parameter 'url'")
		return
	}

	//get optional parameters
	options = &Html2PdfOptions{
		Collate: true,
		Copies:  1,
	}

	//get gray
	grayStr := utils.GetParam(cmd, "gray/[0|1]", "gray")
	if grayStr != "" {
		grayInt, _ := strconv.Atoi(grayStr)
		if grayInt == 1 {
			options.Gray = true
		}
	}

	//get low quality
	lowStr := utils.GetParam(cmd, "low/[0|1]", "low")
	if lowStr != "" {
		lowInt, _ := strconv.Atoi(lowStr)
		if lowInt == 1 {
			options.LowQuality = true
		}
	}

	//orient
	options.Orientation = utils.GetParam(cmd, "orient/(Portrait|Landscape)", "orient")

	//size
	options.Size = utils.GetParam(cmd, "size/[A-B][0-8]", "size")

	//title
	title, decodeErr := utils.GetParamDecoded(cmd, "title/[0-9a-zA-Z-_=]+", "title")
	if decodeErr != nil {
		err = errors.New("invalid html2pdf parameter 'title'")
		return
	}
	options.Title = title

	//collate
	collateStr := utils.GetParam(cmd, "collate/[0|1]", "collate")
	if collateStr != "" {
		collateInt, _ := strconv.Atoi(collateStr)
		if collateInt == 0 {
			options.Collate = false
		}
	}

	//copies
	copiesStr := utils.GetParam(cmd, `copies/\d+`, "copies")
	if copiesStr != "" {
		copiesInt, _ := strconv.Atoi(copiesStr)
		if copiesInt <= 0 {
			err = errors.New("invalid html2pdf parameter 'copies'")
			return
		} else {
			options.Copies = copiesInt
		}
	}

	return
}

func (this *Html2Pdfer) Do(req ufop.UfopRequest) (result interface{}, resultType int, contentType string, err error) {
	reqId := req.ReqId
	remoteSrcUrl, options, pErr := this.parse(req.Cmd)
	if pErr != nil {
		err = pErr
		return
	}

	//if not text format, error it
	if !strings.HasPrefix(req.Src.MimeType, "text/") {
		err = errors.New("unsupported file mime type, only text/* allowed")
		return
	}

	//if file size exceeds, error it
	if req.Src.Fsize > this.maxPageSize {
		err = errors.New("page file length exceeds the limit")
		return
	}

	if options.Copies > this.maxCopies {
		err = errors.New("pdf copies exceeds the limit")
		return
	}

	jobPrefix := utils.Md5Hex(req.Src.Url)

	//prepare command
	cmdParams := make([]string, 0)
	cmdParams = append(cmdParams, "-q")

	if options.Gray {
		cmdParams = append(cmdParams, "--grayscale")
	}

	if options.LowQuality {
		cmdParams = append(cmdParams, "--lowquality")
	}

	if options.Orientation != "" {
		cmdParams = append(cmdParams, "--orientation", options.Orientation)
	}

	if options.Size != "" {
		cmdParams = append(cmdParams, "--page-size", options.Size)
	}

	if options.Title != "" {
		cmdParams = append(cmdParams, "--title", options.Title)
	}

	if options.Collate {
		cmdParams = append(cmdParams, "--collate")
	} else {
		cmdParams = append(cmdParams, "--no-collate")
	}

	cmdParams = append(cmdParams, "--copies", fmt.Sprintf("%d", options.Copies))

	//result tmp file
	resultTmpFname := fmt.Sprintf("%s%d.result.pdf", jobPrefix, time.Now().UnixNano())
	resultTmpFpath := filepath.Join(os.TempDir(), resultTmpFname)

	cmdParams = append(cmdParams, remoteSrcUrl, resultTmpFpath)

	//cmd
	convertCmd := exec.Command("wkhtmltopdf", cmdParams...)
	log.Info(reqId, convertCmd.Path, convertCmd.Args)

	stdErrPipe, pipeErr := convertCmd.StderrPipe()
	if pipeErr != nil {
		err = errors.New(fmt.Sprintf("open exec stderr pipe error, %s", pipeErr.Error()))
		return
	}

	if startErr := convertCmd.Start(); startErr != nil {
		err = errors.New(fmt.Sprintf("start html2pdf command error, %s", startErr.Error()))
		return
	}

	stdErrData, readErr := ioutil.ReadAll(stdErrPipe)
	if readErr != nil {
		err = errors.New(fmt.Sprintf("read html2pdf command stderr error, %s", readErr.Error()))
		defer os.Remove(resultTmpFpath)
		return
	}

	//check stderr output & output file
	if string(stdErrData) != "" {
		log.Info(reqId, string(stdErrData))
	}

	if waitErr := convertCmd.Wait(); waitErr != nil {
		err = errors.New(fmt.Sprintf("wait html2pdf to exit error, %s", waitErr.Error()))
		defer os.Remove(resultTmpFpath)
		return
	}

	if oFileInfo, statErr := os.Stat(resultTmpFpath); statErr != nil || oFileInfo.Size() == 0 {
		err = errors.New("html2pdf with no valid output result")
		defer os.Remove(resultTmpFpath)
		return
	}

	//write result
	result = resultTmpFpath
	resultType = ufop.RESULT_TYPE_OCTECT_FILE
	contentType = "application/pdf"
	return
}
