package pipeline

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type Indexer struct {
	client  *http.Client
	baseURL string
	apiKey  string
}

type DeliveryMechanism struct {
	Adapter    string           `json:"adapter"`
	Connection connectionObject `json:"connection"`
}

type connectionObject struct {
	Host    string            `json:"host"`
	Headers map[string]string `json:"headers"`
}

func NewDeliveryMechanism(adapter, host string, headers map[string]string) *DeliveryMechanism {
	return &DeliveryMechanism{
		Adapter: adapter,
		Connection: connectionObject{
			Host:    host,
			Headers: headers,
		},
	}
}

func NewIndexer(apiKey, url string) *Indexer {
	return &Indexer{
		baseURL: url,
		apiKey:  apiKey,
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

func (i *Indexer) CreateFilter(name string, values []string) (string, error) {
	url := fmt.Sprintf("%s/filters/%s", i.baseURL, name)
	fmt.Println("url", url)
	body, err := json.Marshal(map[string][]string{
		"values": values,
	})
	if err != nil {
		err = fmt.Errorf("json marshal error: %v", err)
		return "", err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return "", err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-API-KEY", i.apiKey)

	resp, err := i.client.Do(req)
	if err != nil {
		err = fmt.Errorf("[HTTP] client error: %v", err)
		return "", err
	}

	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	fmt.Println("status:", resp.Status)
	fmt.Println("response body:", string(bodyBytes))

	return name, nil
}

func (i *Indexer) ListFilters(name string) {}

func (i *Indexer) CreateTransformation(name, code string) (string, error) {
	url := fmt.Sprintf("%s/transformations/%s", i.baseURL, name)

	body, err := json.Marshal(map[string]string{
		"code": code,
	})
	if err != nil {
		err = fmt.Errorf("json marshal error: %v", err)
		return "", err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return "", err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-API-KEY", i.apiKey)

	resp, err := i.client.Do(req)
	if err != nil {
		err = fmt.Errorf("[HTTP] client error: %v", err)
		return "", err
	}

	defer resp.Body.Close()
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %v", err)
	}

	fmt.Println("status:", resp.Status)
	fmt.Println("response body:", string(bodyBytes))
	return name, nil
}

func (i *Indexer) CreatePipeline(name, transformation, filter string, filterKeys, networks []string, delivery *DeliveryMechanism) error {
	url := fmt.Sprintf("%s/pipelines", i.baseURL)
	data := map[string]any{
		"name":           name,
		"transformation": transformation,
		"filter":         filter,
		"filterKeys":     filterKeys,
		"networks":       networks,
		"delivery":       delivery,
	}
	body, err := json.Marshal(data)
	if err != nil {
		err = fmt.Errorf("json marshal error: %v", err)
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("X-API-KEY", i.apiKey)

	resp, err := i.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("failed to read response body: %v", err)
	}

	fmt.Println("status:", resp.Status)
	fmt.Println("response body:", string(bodyBytes))
	return nil
}

func (i *Indexer) BackfillHistorical() {}
