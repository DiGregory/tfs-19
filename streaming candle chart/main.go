package main

import (
	"time"
	"os"
	"encoding/csv"
	"bufio"
	"log"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sort"
	"flag"
	"io"
	"context"
)

type tradeRecord struct {
	Ticker string
	Price  float64
	Count  int
	Date   time.Time
}
type candle struct {
	Ticker       string
	Date         time.Time
	OpeningPrice float64
	MaxPrice     float64
	MinPrice     float64
	ClosingPrice float64
}

func readFile(ctx context.Context, filename string) (<-chan tradeRecord) {
	out := make(chan tradeRecord)
	tradesCSV, err := os.Open(filename)
	if err != nil {
		log.Fatal("can`t open: ", err)
	}

	reader := csv.NewReader(bufio.NewReader(tradesCSV))

	go func(file *os.File) {
		defer file.Close()
		defer close(out)
		for {
			line, err := reader.Read()
			switch {
			case err == io.EOF:
				return
			case err != nil:
				fmt.Println("can`t read the line: ", err)
			}

			date := line[3]
			d := strings.Split(date, " ")[0]
			t := strings.Split(date, " ")[1]
			dateRFC3339, err := time.Parse(time.RFC3339, d+"T"+t+"Z")
			if err != nil {
				fmt.Println("can`t parse the time: ", err)
			}
			price, err := strconv.ParseFloat(line[1], 64)
			if err != nil {
				fmt.Println("can`t parse the price: ", err)
			}
			count, err := strconv.Atoi(line[2])
			if err != nil {
				fmt.Println("can`t parse the count: ", err)
			}
			trade := tradeRecord{
				Ticker: line[0],
				Price:  price,
				Count:  count,
				Date:   dateRFC3339,
			}
			select {
			case out <- trade:
			case <-ctx.Done():
				fmt.Println("Exit: pipeline timeout")
				return
			}

		}

	}(tradesCSV)

	return out
}

func splitTrades(in <-chan tradeRecord) (<-chan tradeRecord, <-chan tradeRecord, <-chan tradeRecord) {
	out1 := make(chan tradeRecord)
	out2 := make(chan tradeRecord)
	out3 := make(chan tradeRecord)

	go func(<-chan tradeRecord, <-chan tradeRecord, <-chan tradeRecord) {
		defer close(out1)
		defer close(out2)
		defer close(out3)
		for v := range in {
			out1 <- v
			out2 <- v
			out3 <- v
		}
	}(out1, out2, out3)
	return out1, out2, out3
}
func maxPrice(tr []tradeRecord) float64 {
	max := tr[0].Price
	for _, v := range tr {
		if v.Price > max {
			max = v.Price
		}
	}
	return max
}

func minPrice(tr []tradeRecord) float64 {
	min := tr[0].Price
	for _, v := range tr {
		if v.Price < min {
			min = v.Price
		}
	}
	return min
}
func candleConverting(trades []tradeRecord, start time.Time) (candle) {
	candle := candle{
		Ticker:       trades[0].Ticker,
		OpeningPrice: trades[0].Price,
		MaxPrice:     maxPrice(trades),
		MinPrice:     minPrice(trades),
		ClosingPrice: trades[len(trades)-1].Price,
		Date:         start,
	}
	return candle
}
func createCandles(tickers map[string][]tradeRecord, start time.Time) ([]candle) {
	var c candle
	var candles []candle
	for _, v := range tickers {
		c = candleConverting(v, start)
		candles = append(candles, c)
	}
	sort.Slice(candles, func(i, j int) bool {
		return candles[i].MinPrice < candles[j].MinPrice
	})
	return candles
}

func candlesBuild(in <-chan tradeRecord, out chan []candle, step time.Duration) {
	tickers := make(map[string][]tradeRecord)
	var start time.Time
	var err error

	for trade := range in {
		if trade.Date.Hour() >= 3 && trade.Date.Hour() < 7 {
			candles := createCandles(tickers, start)
			day := strings.Split(trade.Date.String(), " ")[0]
			start, err = time.Parse(time.RFC3339, day+"T07:00:00Z")
			if err != nil {
				fmt.Println("can`t parse the time: ", err)
			}
			out <- candles
			tickers = map[string][]tradeRecord{}
			continue
		}
		if start.Add(step).Before(trade.Date) {
			candles := createCandles(tickers, start)
			out <- candles
			tickers = map[string][]tradeRecord{}
			start = start.Add(step)
		}
		tickers[trade.Ticker] = append(tickers[trade.Ticker], trade)
	}
	candles := createCandles(tickers, start)
	out <- candles
}

func makeCandles(in1, in2, in3 <-chan tradeRecord) (<-chan []candle, <-chan []candle, <-chan []candle) {
	out1 := make(chan []candle)
	out2 := make(chan []candle)
	out3 := make(chan []candle)
	candleBuilder := func(in <-chan tradeRecord, out chan []candle, step time.Duration) {
		defer close(out)
		candlesBuild(in, out, step)

	}
	go candleBuilder(in1, out1, 5*time.Minute)
	go candleBuilder(in2, out2, 30*time.Minute)
	go candleBuilder(in3, out3, 240*time.Minute)
	return out1, out2, out3
}
func writeToFile(in <-chan []candle, filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	for candles := range in {
		if len(candles) == 0 {
			continue
		}
		for _, candle := range candles {
			str := candleToString(candle)
			if _, err := file.WriteString(str); err != nil {
				log.Fatal(err)
			}
		}
	}

}
func candleToString(candle candle) (str string) {
	date := candle.Date.Format(time.RFC3339)
	openingPrice := strconv.FormatFloat(candle.OpeningPrice, 'f', -1, 64)
	maxPrice := strconv.FormatFloat(candle.MaxPrice, 'f', -1, 64)
	minPrice := strconv.FormatFloat(candle.MinPrice, 'f', -1, 64)
	closingPrice := strconv.FormatFloat(candle.ClosingPrice, 'f', -1, 64)
	str = fmt.Sprintf("%s,%s,%s,%s,%s,%s\n", candle.Ticker, date, openingPrice, maxPrice, minPrice, closingPrice)
	return str
}

func writeCandlesToFiles(in1, in2, in3 <-chan []candle) {

	var wg sync.WaitGroup
	wg.Add(3)
	writeToFile := func(filename string, in <-chan []candle) {
		defer wg.Done()
		writeToFile(in, filename)
	}
	go writeToFile("candles_5m.csv", in1)
	go writeToFile("candles_30m.csv", in2)
	go writeToFile("candles_240m.csv", in3)
	wg.Wait()

}
func startPipeline() {
	waitTime := 5000 * time.Millisecond
	ctx, finish := context.WithTimeout(context.Background(), waitTime)
	defer finish()
	var filename string
	flag.StringVar(&filename, "file", "", "")
	flag.Parse()
	fileReadingChan := readFile(ctx, filename)
	c1, c2, c3 := splitTrades(fileReadingChan)
	candlesChan1, candlesChan2, candlesChan3 := makeCandles(c1, c2, c3)
	writeCandlesToFiles(candlesChan1, candlesChan2, candlesChan3)
}

func main() {
	startPipeline()
}
