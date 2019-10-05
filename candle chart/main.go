package main

import (
	"fmt"
	"os"
	"bufio"
	"strings"
	"strconv"
)

type ExchangeTransaction struct {
	Ticker string
	Sale   string
	Count  string
	Date   string
	Time   string
}

func Time(time string) float64 {
	t := strings.Split(time, ":")
	h, _ := strconv.ParseFloat(t[0], 64)
	m, _ := strconv.ParseFloat(t[1], 64)
	s, _ := strconv.ParseFloat(t[2], 64)

	return h*3600 + m*60 + s
}

func Max(arr []ExchangeTransaction) string {
	max := arr[0].Sale
	for _, v := range arr {
		if v.Sale > max {
			max = v.Sale
		}
	}
	return max
}

func Min(arr []ExchangeTransaction) string {
	min := arr[0].Sale
	for _, v := range arr {
		if v.Sale < min {
			min = v.Sale
		}
	}
	return min
}

func Write(trades []ExchangeTransaction, timeStep, filename string) {
	open := trades[0].Sale
	high := Max(trades)
	low  := Min(trades)
	shut := trades[len(trades)-1].Sale
	var time string
	if timeStep == "04:00:00" {
		time = trades[0].Time[0:2] + timeStep[2:5] + ":00"
	} else {
		minutes, _ := strconv.Atoi(trades[0].Time[3:5])
		step, _ := strconv.Atoi(timeStep[3:5])
		remain := minutes % step
		if remain == 0 {
			time = strconv.Itoa(minutes)
			if len(time) < 2 {
				time = "0" + time
			}
		} else {
			time = strconv.Itoa(minutes - remain)
			if len(time) < 2 {
				time = "0" + time
			}
		}
		//fmt.Printf("%d %d %d\n", remain, minutes, step)
		time = trades[0].Time[0:2] + ":" + time + ":00"
	}

	entry := fmt.Sprintf("%s,%sT%sZ,%s,%s,%s,%s\n", trades[0].Ticker, trades[0].Date, time ,open, high, low, shut)
	//fmt.Println(entry)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("%+v\n", err)
		return
	}
	defer file.Close()
	if _, err := file.WriteString(entry); err != nil {
		fmt.Println("%+v\n", err)
		return
	}
}

func Drain(TickerTable map[string][]ExchangeTransaction, timeStep, filename string) {
	val, ok := TickerTable["AAPL"]
	if ok {
		Write(val, timeStep, filename)
	}
	val, ok = TickerTable["SBER"]
	if ok {
		Write(val, timeStep, filename)
	}
	val, ok = TickerTable["AMZN"]
	if ok {
		Write(val, timeStep, filename)
	}
}

func Trading(timeStart, timeStep, filename string) {
	file, err := os.Open("trades.csv")
	if err != nil {
		fmt.Printf("%+v\n", err)
		return
	}
	defer file.Close()

	scan  := bufio.NewScanner(file)
	start := Time(timeStart)
	step  := Time(timeStep)
	TickerTable := make(map[string][]ExchangeTransaction)
	for scan.Scan() {
		line := scan.Text()
		cols := strings.Split(line, ",")
		date := strings.Split(cols[len(cols)-1], " ")
		time := Time(date[1])
		if "03:00:00" <= date[1] && date[1] < "07:00:00" {
			start = Time("07:00:00")
			Drain(TickerTable, timeStep, filename)
			TickerTable = map[string][]ExchangeTransaction{} //Clearing the map
			continue
		}
//		else if "07:00:00" <= date[1] && date[1] < "07:05:00" {
//			//start = Time("07:00:00")
//		}

		if time >= start + step {
			Drain(TickerTable, timeStep, filename)
			TickerTable = map[string][]ExchangeTransaction{} //Clearing the map
			start += step
		}

		trans := ExchangeTransaction{
			Ticker: cols[0],
			Sale:   cols[1],
			Count:  cols[2],
			Date:   date[0],
			Time:   date[1],
		}
		switch trans.Ticker{
		case "SBER":
			TickerTable["SBER"] = append(TickerTable["SBER"], trans)
		case "AAPL":
			TickerTable["AAPL"] = append(TickerTable["AAPL"], trans)
		case "AMZN":
			TickerTable["AMZN"] = append(TickerTable["AMZN"], trans)
		}
	}
	//fmt.Printf("%+v\n", TickerTable)
	Drain(TickerTable, timeStep, filename)
}


func main() {
	Trading("07:00:00", "04:00:00", "candles_240min.csv")
	Trading("07:00:00", "00:05:00", "candles_5min.csv")
	Trading("07:00:00", "00:30:00", "candles_30min.csv")
}
