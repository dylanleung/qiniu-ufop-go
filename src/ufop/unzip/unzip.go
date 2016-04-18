package unzip

import (
	"archive/zip"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/qiniu/api.v6/auth/digest"
	"github.com/qiniu/api.v6/conf"
	fio "github.com/qiniu/api.v6/io"
	rio "github.com/qiniu/api.v6/resumable/io"
	"github.com/qiniu/api.v6/rs"
	"github.com/qiniu/log"
	"io/ioutil"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"ufop"
	"ufop/utils"
	"unicode/utf8"
)

const (
	UNZIP_MAX_ZIP_FILE_LENGTH uint64 = 1 * 1024 * 1024 * 1024
	UNZIP_MAX_FILE_LENGTH     uint64 = 100 * 1024 * 1024 //100MB
	UNZIP_MAX_FILE_COUNT      int    = 10                //10

	MAX_UPLOAD_WORKERS = 100
)

type UnzipResult struct {
	Files []UnzipFile `json:"files"`
}

type UnzipFile struct {
	Key   string `json:"key"`
	Hash  string `json:"hash,omitempty"`
	Error string `json:"error,omitempty"`
}

type Unzipper struct {
	mac              *digest.Mac
	maxZipFileLength uint64
	maxFileLength    uint64
	maxFileCount     int
}

type UnzipperConfig struct {
	//ak & sk
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`

	UnzipMaxZipFileLength uint64 `json:"unzip_max_zip_file_length,omitempty"`
	UnzipMaxFileLength    uint64 `json:"unzip_max_file_length,omitempty"`
	UnzipMaxFileCount     int    `json:"unzip_max_file_count,omitempty"`
}

func (this *Unzipper) Name() string {
	return "unzip"
}

func (this *Unzipper) InitConfig(jobConf string) (err error) {
	confFp, openErr := os.Open(jobConf)
	if openErr != nil {
		err = errors.New(fmt.Sprintf("Open unzip config failed, %s", openErr.Error()))
		return
	}

	config := UnzipperConfig{}
	decoder := json.NewDecoder(confFp)
	decodeErr := decoder.Decode(&config)
	if decodeErr != nil {
		err = errors.New(fmt.Sprintf("Parse unzip config failed, %s", decodeErr.Error()))
		return
	}

	if config.UnzipMaxFileCount <= 0 {
		this.maxFileCount = UNZIP_MAX_FILE_COUNT
	} else {
		this.maxFileCount = config.UnzipMaxFileCount
	}

	if config.UnzipMaxFileLength <= 0 {
		this.maxFileLength = UNZIP_MAX_FILE_LENGTH
	} else {
		this.maxFileLength = config.UnzipMaxFileLength
	}

	if config.UnzipMaxZipFileLength <= 0 {
		this.maxZipFileLength = UNZIP_MAX_ZIP_FILE_LENGTH
	} else {
		this.maxZipFileLength = config.UnzipMaxZipFileLength
	}

	this.mac = &digest.Mac{config.AccessKey, []byte(config.SecretKey)}

	return
}

/*

unzip/bucket/<encoded bucket>/prefix/<encoded prefix>/overwrite/<[0|1]>

*/
func (this *Unzipper) parse(cmd string) (bucket string, prefix string, overwrite bool, err error) {
	pattern := "^unzip/bucket/[0-9a-zA-Z-_=]+(/prefix/[0-9a-zA-Z-_=]+){0,1}(/overwrite/(0|1)){0,1}$"
	matched, _ := regexp.MatchString(pattern, cmd)
	if !matched {
		err = errors.New("invalid unzip command format")
		return
	}

	var decodeErr error
	bucket, decodeErr = utils.GetParamDecoded(cmd, "bucket/[0-9a-zA-Z-_=]+", "bucket")
	if decodeErr != nil {
		err = errors.New("invalid unzip parameter 'bucket'")
		return
	}
	prefix, decodeErr = utils.GetParamDecoded(cmd, "prefix/[0-9a-zA-Z-_=]+", "prefix")
	if decodeErr != nil {
		err = errors.New("invalid unzip parameter 'prefix'")
		return
	}
	overwriteStr := utils.GetParam(cmd, "overwrite/(0|1)", "overwrite")
	if overwriteStr != "" {
		overwriteVal, paramErr := strconv.ParseInt(overwriteStr, 10, 64)
		if paramErr != nil {
			err = errors.New("invalid unzip parameter 'overwrite'")
			return
		}
		if overwriteVal == 1 {
			overwrite = true
		}
	}
	return
}

func (this *Unzipper) Do(req ufop.UfopRequest) (result interface{}, resultType int, contentType string, err error) {
	//parse command
	bucket, prefix, overwrite, pErr := this.parse(req.Cmd)
	if pErr != nil {
		err = pErr
		return
	}

	//check mimetype
	if !(req.Src.MimeType == "application/zip" || req.Src.MimeType == "application/x-zip-compressed") {
		err = errors.New("unsupported mimetype to unzip")
		return
	}
	//check zip file length
	if req.Src.Fsize > this.maxZipFileLength {
		err = errors.New("src zip file length exceeds the limit")
		return
	}

	log.Infof("[%s] downloading file", req.ReqId)
	//get resource
	resUrl := req.Src.Url
	resResp, respErr := http.Get(resUrl)
	if respErr != nil || resResp.StatusCode != 200 {
		if respErr != nil {
			err = errors.New(fmt.Sprintf("retrieve resource data failed, %s", respErr.Error()))
		} else {
			err = errors.New(fmt.Sprintf("retrieve resource data failed, %s", resResp.Status))
			if resResp.Body != nil {
				resResp.Body.Close()
			}
		}
		return
	}
	defer resResp.Body.Close()

	respData, readErr := ioutil.ReadAll(resResp.Body)
	if readErr != nil {
		err = errors.New(fmt.Sprintf("read resource data failed, %s", readErr.Error()))
		return
	}

	log.Infof("[%s] trying to read zip", req.ReqId)
	//read zip
	respReader := bytes.NewReader(respData)
	zipReader, zipErr := zip.NewReader(respReader, int64(respReader.Len()))
	if zipErr != nil {
		err = errors.New(fmt.Sprintf("invalid zip file, %s", zipErr.Error()))
		return
	}
	zipFiles := zipReader.File
	//check file count
	zipFileCount := len(zipFiles)
	if zipFileCount > this.maxFileCount {
		err = errors.New("zip files count exceeds the limit")
		return
	}
	//check file size
	for _, zipFile := range zipFiles {
		fileSize := zipFile.UncompressedSize64
		//check file size
		if fileSize > this.maxFileLength {
			err = errors.New("zip file length exceeds the limit")
			return
		}
	}

	log.Infof("[%s] start to upload files", req.ReqId)
	//set up host
	conf.UP_HOST = "http://up.qiniu.com"
	rputSettings := rio.Settings{
		ChunkSize: 4 * 1024 * 1024,
		Workers:   1,
	}
	rio.SetSettings(&rputSettings)
	var rputThreshold uint64 = 100 * 1024 * 1024
	policy := rs.PutPolicy{
		Scope: bucket,
	}
	var unzipResult UnzipResult
	unzipResult.Files = make([]UnzipFile, 0, 100)
	var tErr error
	//iterate the zip file
	uploadWg := sync.WaitGroup{}
	resultLock := sync.RWMutex{}
	uploadCounter := 0
	for _, zipFile := range zipFiles {
		fileInfo := zipFile.FileHeader.FileInfo()
		fileName := zipFile.FileHeader.Name
		fileSize := zipFile.UncompressedSize64

		if !utf8.Valid([]byte(fileName)) {
			fileName, tErr = utils.Gbk2Utf8(fileName)
			if tErr != nil {
				err = errors.New(fmt.Sprintf("unsupported file name encoding, %s", tErr.Error()))
				return
			}
		}

		if fileInfo.IsDir() {
			continue
		}

		zipFileReader, zipErr := zipFile.Open()
		if zipErr != nil {
			err = errors.New(fmt.Sprintf("open zip file content failed, %s", zipErr.Error()))
			return
		}
		defer zipFileReader.Close()

		unzipData, unzipErr := ioutil.ReadAll(zipFileReader)
		if unzipErr != nil {
			err = errors.New(fmt.Sprintf("unzip the file content failed, %s", unzipErr.Error()))
			return
		}
		unzipReader := bytes.NewReader(unzipData)

		//save file to bucket
		fileName = prefix + fileName
		if overwrite {
			policy.Scope = bucket + ":" + fileName
		}
		uptoken := policy.Token(this.mac)
		var unzipFile UnzipFile
		unzipFile.Key = fileName

		//incr counter
		uploadCounter += 1

		if uploadCounter%MAX_UPLOAD_WORKERS == 0 {
			uploadWg.Wait()
		}

		uploadWg.Add(1)

		go func() {
			defer uploadWg.Done()
			fmt.Println(fileName)
			if fileSize <= rputThreshold {
				var fputRet fio.PutRet
				fErr := fio.Put(nil, &fputRet, uptoken, fileName, unzipReader, nil)
				if fErr != nil {
					unzipFile.Error = fmt.Sprintf("save unzip file to bucket error, %s", fErr.Error())
				} else {
					unzipFile.Hash = fputRet.Hash
				}

			} else {
				var rputRet rio.PutRet
				rErr := rio.Put(nil, &rputRet, uptoken, fileName, unzipReader, int64(fileSize), nil)
				if rErr != nil {
					unzipFile.Error = fmt.Sprintf("save unzip file to bucket error, %s", rErr.Error())
				} else {
					unzipFile.Hash = rputRet.Hash
				}
			}

			resultLock.Lock()
			unzipResult.Files = append(unzipResult.Files, unzipFile)
			resultLock.Unlock()
		}()
	}

	uploadWg.Wait()
	log.Infof("[%s] upload files done", req.ReqId)
	//write result
	result = unzipResult
	resultType = ufop.RESULT_TYPE_JSON
	contentType = ufop.CONTENT_TYPE_JSON

	return
}
