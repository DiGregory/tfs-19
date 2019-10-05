package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)

type News struct {
	ID           int64     `json:"id"`
	Title        string    `json:"title"`
	Body         string    `json:"body"`
	Provider     string    `json:"provider"`
	PublishedAt  time.Time `json:"published_at"`
	Tickers      []string  `json:"tickers"`
}

type Group struct {
	Items       []News    `json:"items"`
	PublishedAt time.Time `json:"published_at"`
	Tickers     []string  `json:"tickers"`
}

type Feed struct {
	Type        string      `json:"type"`
	Payload     interface{} `json:"payload"`
	publication time.Time
}

func ReadNews(filename string) ([]News, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("can't open file: %s", err)
	}
	defer file.Close()
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("can't read file: %s", err)
	}
	var news []News
	err = json.Unmarshal([]byte(bytes), &news)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshal: %s", err)
	}

	return news, nil
}

func yearMonthDay(t time.Time) string {
	date := strings.Split(t.String(), " ")
	return date[0]
}

func drainNewsOfCurrDay(newsByDate map[string][]News, feeds []Feed) []Feed {
	for _, slice := range newsByDate {
		if len(slice) == 1 {
			out := Feed{
				Type:        "news",
				Payload:     slice[0],
				publication: slice[0].PublishedAt,
			}
			feeds = append(feeds, out)
			continue
		}
		group := Group{
			Items:       slice,
			PublishedAt: slice[0].PublishedAt,
			Tickers:     slice[0].Tickers,
		}
		outs := Feed{
			Type:        "company_news",
			Payload:     group,
			publication: slice[0].PublishedAt,
		}
		feeds = append(feeds, outs)
	}

	return feeds
}

func MakeFeed(news []News) []Feed {
	if len(news) == 0 {
		return nil
	}
	sort.SliceStable(news, func(i, j int) bool {
		return news[i].PublishedAt.Before(news[j].PublishedAt)
	})
	date := yearMonthDay(news[0].PublishedAt)
	newsByDate := make(map[string][]News)
	var output []Feed
	for i, val := range news {
		currDate := yearMonthDay(val.PublishedAt)
		if currDate != date {
			date = yearMonthDay(val.PublishedAt)
			output = drainNewsOfCurrDay(newsByDate, output)
			newsByDate = map[string][]News{}
		}
		tmpTickers := make([]string, len(val.Tickers))
		copy(tmpTickers, val.Tickers)
		sort.Strings(tmpTickers)
		tickers := strings.Join(tmpTickers, "_")
		newsByDate[tickers] = append(newsByDate[tickers], val)
		if i == len(news)-1 {
			output = drainNewsOfCurrDay(newsByDate, output)
		}
	}
	sort.SliceStable(output, func(i, j int) bool {
		entryI := output[i].publication
		entryJ := output[j].publication
		return entryI.Before(entryJ)
	})

	return output
}

func main() {
	var filename string
	flag.StringVar(&filename, "file", "", "")
	flag.Parse()
	news, err := ReadNews(filename)
	if err != nil {
		log.Fatalf("can't read news: %s", err)
	}
	output := MakeFeed(news)
	js, err := json.MarshalIndent(output, "", "    ")
	if err != nil {
		log.Fatalf("can't marshal: %s", err)
	}
	if err := ioutil.WriteFile("./out.json", []byte(js), 0666); err != nil {
		log.Fatalf("can't wtite json to file: %s", err)
	}
}
