package html2image

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
	HTML2IMAGE_MAX_PAGE_SIZE = 10 * 1024 * 1024
)

type Html2Imager struct {
	maxPageSize uint64
}

type Html2ImagerConfig struct {
	Html2ImageMaxPageSize uint64 `json:"html2image_max_page_size,omitempty"`
}

type Html2ImageOptions struct {
	CropH   int
	CropW   int
	CropX   int
	CropY   int
	Format  string
	Height  int
	Width   int
	Quality int
	Force   bool
}

func (this *Html2Imager) Name() string {
	return "html2image"
}

func (this *Html2Imager) InitConfig(jobConf string) (err error) {
	confFp, openErr := os.Open(jobConf)
	if openErr != nil {
		err = errors.New(fmt.Sprintf("Open html2image config failed, %s", openErr.Error()))
		return
	}

	config := Html2ImagerConfig{}
	decoder := json.NewDecoder(confFp)
	decodeErr := decoder.Decode(&config)
	if decodeErr != nil {
		err = errors.New(fmt.Sprintf("Parse html2image config failed, %s", decodeErr.Error()))
		return
	}

	if config.Html2ImageMaxPageSize <= 0 {
		this.maxPageSize = HTML2IMAGE_MAX_PAGE_SIZE
	} else {
		this.maxPageSize = config.Html2ImageMaxPageSize
	}

	return
}

func (this *Html2Imager) parse(cmd string) (url string, options *Html2ImageOptions, err error) {
	pattern := `^html2image/url/[0-9a-zA-Z-_=]+(/croph/\d+|/cropw/\d+|/cropx/\d+|/cropy/\d+|/format/(png|jpg|jpeg)|/height/\d+|/quality/\d+|/width/\d+|/force/[0|1]){0,9}$`
	matched, _ := regexp.MatchString(pattern, cmd)
	if !matched {
		err = errors.New("invalid html2image command format")
		return
	}

	options = &Html2ImageOptions{
		Format: "jpg",
	}

	//url
	var decodeErr error
	url, decodeErr = utils.GetParamDecoded(cmd, `url/[0-9a-zA-Z-_=]+`, "url")
	if decodeErr != nil {
		err = errors.New("invalid html2image parameter 'url'")
		return
	}

	//croph
	cropHStr := utils.GetParam(cmd, `croph/\d+`, "croph")
	if cropHStr != "" {
		cropH, _ := strconv.Atoi(cropHStr)
		if cropH <= 0 {
			err = errors.New("invalid html2image parameter 'croph'")
			return
		} else {
			options.CropH = cropH
		}
	}

	//cropw
	cropWStr := utils.GetParam(cmd, `cropw/\d+`, "cropw")
	if cropWStr != "" {
		cropW, _ := strconv.Atoi(cropWStr)
		if cropW <= 0 {
			err = errors.New("invalid html2image parameter 'cropw'")
			return
		} else {
			options.CropW = cropW
		}
	}

	//cropx
	cropXStr := utils.GetParam(cmd, `cropx/\d+`, "cropx")
	fmt.Println(cropXStr)
	if cropXStr != "" {
		cropX, _ := strconv.Atoi(cropXStr)
		if cropX <= 0 {
			err = errors.New("invalid html2image parameter 'cropx'")
			return
		} else {
			options.CropX = cropX
		}
	}

	//cropy
	cropYStr := utils.GetParam(cmd, `cropy/\d+`, "cropy")
	if cropYStr != "" {
		cropY, _ := strconv.Atoi(cropYStr)
		if cropY <= 0 {
			err = errors.New("invalid html2image parameter 'cropy'")
			return
		} else {
			options.CropY = cropY
		}
	}

	//format
	formatStr := utils.GetParam(cmd, "format/(png|jpg|jpeg)", "format")
	if formatStr != "" {
		options.Format = formatStr
	}

	//height
	heightStr := utils.GetParam(cmd, `height/\d+`, "height")
	if heightStr != "" {
		height, _ := strconv.Atoi(heightStr)
		if height <= 0 {
			err = errors.New("invalid html2image parameter 'height'")
			return
		} else {
			options.Height = height
		}
	}

	//width
	widthStr := utils.GetParam(cmd, `width/\d+`, "width")
	if widthStr != "" {
		width, _ := strconv.Atoi(widthStr)
		if width <= 0 {
			err = errors.New("invalid html2image parameter 'width'")
			return
		} else {
			options.Width = width
		}
	}

	//quality
	qualityStr := utils.GetParam(cmd, `quality/\d+`, "quality")
	if qualityStr != "" {
		quality, _ := strconv.Atoi(qualityStr)
		if quality > 100 || quality <= 0 {
			err = errors.New("invalid html2image parameter 'quality'")
			return
		} else {
			options.Quality = quality
		}
	}

	//force
	forceStr := utils.GetParam(cmd, "force/[0|1]", "force")
	if forceStr != "" {
		force, _ := strconv.Atoi(forceStr)
		if force == 1 {
			options.Force = true
		}
	}

	return

}

func (this *Html2Imager) Do(req ufop.UfopRequest) (result interface{}, resultType int, contentType string, err error) {
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

	jobPrefix := utils.Md5Hex(req.Src.Url)

	//prepare command
	cmdParams := make([]string, 0)
	//cmdParams = append(cmdParams, "-q")

	if options.CropH > 0 {
		cmdParams = append(cmdParams, "--crop-h", fmt.Sprintf("%d", options.CropH))
	}

	if options.CropW > 0 {
		cmdParams = append(cmdParams, "--crop-w", fmt.Sprintf("%d", options.CropW))
	}

	if options.CropX > 0 {
		cmdParams = append(cmdParams, "--crop-x", fmt.Sprintf("%d", options.CropX))
	}

	if options.CropY > 0 {
		cmdParams = append(cmdParams, "--crop-y", fmt.Sprintf("%d", options.CropY))
	}

	if options.Format != "" {
		cmdParams = append(cmdParams, "--format", options.Format)
	}

	if options.Quality > 0 {
		cmdParams = append(cmdParams, "--quality", fmt.Sprintf("%d", options.Quality))
	}

	if options.Height > 0 {
		cmdParams = append(cmdParams, "--height", fmt.Sprintf("%d", options.Height))
	}

	if options.Width > 0 {
		cmdParams = append(cmdParams, "--width", fmt.Sprintf("%d", options.Width))
	}

	if options.Force {
		cmdParams = append(cmdParams, "--disable-smart-width")
	}

	//result tmp file
	resultTmpFname := fmt.Sprintf("%s%d.result.%s", jobPrefix, time.Now().UnixNano(), options.Format)
	resultTmpFpath := filepath.Join(os.TempDir(), resultTmpFname)

	cmdParams = append(cmdParams, remoteSrcUrl, resultTmpFpath)

	//cmd
	convertCmd := exec.Command("wkhtmltoimage", cmdParams...)
	log.Info(reqId, convertCmd.Path, convertCmd.Args)

	stdErrPipe, pipeErr := convertCmd.StderrPipe()
	if pipeErr != nil {
		err = errors.New(fmt.Sprintf("open exec stderr pipe error, %s", pipeErr.Error()))
		return
	}

	if startErr := convertCmd.Start(); startErr != nil {
		err = errors.New(fmt.Sprintf("start html2image command error, %s", startErr.Error()))
		return
	}

	stdErrData, readErr := ioutil.ReadAll(stdErrPipe)
	if readErr != nil {
		err = errors.New(fmt.Sprintf("read html2image command stderr error, %s", readErr.Error()))
		defer os.Remove(resultTmpFpath)
		return
	}

	//check stderr output & output file
	if string(stdErrData) != "" {
		log.Info(reqId, string(stdErrData))
	}

	if waitErr := convertCmd.Wait(); waitErr != nil {
		err = errors.New(fmt.Sprintf("wait html2image to exit error, %s", waitErr.Error()))
		defer os.Remove(resultTmpFpath)
		return
	}

	if oFileInfo, statErr := os.Stat(resultTmpFpath); statErr != nil || oFileInfo.Size() == 0 {
		err = errors.New("html2image with no valid output result")
		defer os.Remove(resultTmpFpath)
		return
	}

	//write result
	result = resultTmpFpath
	resultType = ufop.RESULT_TYPE_OCTECT_FILE
	if options.Format == "png" {
		contentType = "image/png"
	} else {
		contentType = "image/jpeg"
	}

	return
}
