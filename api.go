package qlrx

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"

	"github.com/txn2/micro"
	"github.com/txn2/provision"
	"github.com/txn2/tm"
	"go.uber.org/zap"
)

// Config
type Config struct {
	Logger     *zap.Logger
	HttpClient *micro.Client
}

// Api
type Api struct {
	*Config
}

// NewApi
func NewApi(cfg *Config) (*Api, error) {
	a := &Api{Config: cfg}

	return a, nil
}

// Inject posts a JSON payload to a url
func (a *Api) Inject(url string, payload []byte) error {

	req, _ := http.NewRequest("POST", url, bytes.NewReader(payload))
	res, err := a.HttpClient.Http.Do(req)
	if err != nil {
		a.Logger.Warn("Request error", zap.Error(err))
		return err
	}

	if res.StatusCode != 200 {
		return errors.New("inject received a non-200 error code POSTing to " + url)
	}

	return nil
}

// Package forms a JSON object from indexed elements and a corresponding model
func (a *Api) Package(elements []string, model *tm.Model) []byte {

	nOfElms := len(elements)

	// map the elements
	elmMap := make(map[string]interface{})

	// groups of fields
	fieldGroups := make(map[string]map[string]interface{})

	// build the payload by looping through the fields in the model
	for _, field := range model.Fields {

		// should the field be pased and with the index will be in bounds?
		if field.Parse == true && field.Index < nOfElms {

			// omit empty elements
			if elements[field.Index] == "" {
				continue
			}

			// does this field belong to a group?
			if field.Group != "" {
				// create a spot for the field group if it does not exist.
				if _, ok := fieldGroups[field.Group]; !ok {
					fieldGroups[field.Group] = make(map[string]interface{})
				}

				fieldGroups[field.Group][field.MachineName] = elements[field.Index]
			}

			elmMap[field.MachineName] = elements[field.Index]
		}
	}

	// add field groups
	for fgKey, fgValue := range fieldGroups {
		elmMap[fgKey] = fgValue
	}

	payloadJson, _ := json.Marshal(elmMap)

	return payloadJson
}

// GetModel
func (a *Api) GetModel(modelService string, accountId string, modelId string) (*tm.Model, error) {

	url := modelService + "/model/" + accountId + "/" + modelId
	req, _ := http.NewRequest("GET", url, nil)
	res, err := a.HttpClient.Http.Do(req)
	if err != nil {
		a.Logger.Warn("Request error", zap.Error(err))
		return nil, err
	}

	if res.StatusCode == 404 {
		err = errors.New("model not found")
		a.Logger.Warn("Model service returned 404.", zap.Error(err))
		return nil, err
	}

	if res.StatusCode != 200 {
		err = errors.New("unexpected response from model service")
		a.Logger.Warn(err.Error(),
			zap.String("url", url),
			zap.Int("code", res.StatusCode),
		)
		return nil, err
	}

	body, _ := ioutil.ReadAll(res.Body)

	err = res.Body.Close()
	if err != nil {
		a.Logger.Error("Unable to close body.", zap.Error(err))
		return nil, err
	}

	modelResult := &tm.ModelResultAck{}
	err = json.Unmarshal(body, &modelResult)
	if err != nil {
		a.Logger.Warn("failed to unmarshal model response.", zap.Error(err))
		return nil, err
	}

	return &modelResult.Payload.Source, nil
}

// getAsset
func (a *Api) GetAsset(provisionService string, assetIdPrefix string, id string) (*provision.Asset, error) {

	url := provisionService + "/asset/" + assetIdPrefix + id
	req, _ := http.NewRequest("GET", url, nil)
	res, err := a.HttpClient.Http.Do(req)
	if err != nil {
		a.Logger.Warn("Request error", zap.Error(err))
		return nil, err
	}

	if res.StatusCode == 404 {
		err = errors.New("asset not found")
		a.Logger.Warn("Provisioning returned 404.", zap.Error(err))
		return nil, err
	}

	if res.StatusCode != 200 {
		err = errors.New("unexpected response from provisioning")
		a.Logger.Warn(err.Error(),
			zap.String("url", url),
			zap.Int("code", res.StatusCode),
		)
		return nil, err
	}

	body, _ := ioutil.ReadAll(res.Body)

	err = res.Body.Close()
	if err != nil {
		a.Logger.Error("Unable to close body.", zap.Error(err))
		return nil, err
	}

	assetResult := &provision.AssetResultAck{}
	err = json.Unmarshal(body, &assetResult)
	if err != nil {
		a.Logger.Warn("failed to unmarshal asset response.", zap.Error(err))
		return nil, err
	}

	return &assetResult.Payload.Source, nil
}
