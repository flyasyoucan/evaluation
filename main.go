package main

import (
	"os"
	"strconv"

	"github.com/astaxie/beego/logs"

	"fmt"
	"math/rand"
	"time"

	"github.com/influxdata/influxdb/client/v2"
)

const (
	MyDB     = "mydb"
	username = "root"
	password = ""
)

func queryDB(clnt client.Client, cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: MyDB,
	}
	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

func writePoints(clnt client.Client, task int, num int) {

	loop := 1
	if num > 100 {
		loop = num / 100
	}

	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  MyDB,
		Precision: "us",
	})

	index := 0

	for ; loop > 0; loop-- {

		index += 100

		for i := 0; i < 100; i++ {

			tags := map[string]string{
				"ID":           fmt.Sprintf("%d", num),
				"BrokerID":     fmt.Sprintf("%d", 2),
				"UserID":       fmt.Sprintf("%d", index+i),
				"Account":      fmt.Sprintf("615%.2d", index+i),
				"TradeId":      fmt.Sprintf("%d", i),
				"Symbol":       fmt.Sprintf("XAUUSD"),
				"Cmd":          fmt.Sprintf("%d", (index+i)%7),
				"BrikerLots":   fmt.Sprintf("1.2"),
				"StandardLots": fmt.Sprintf("1.3"),
				"Quatity":      fmt.Sprintf("%d", 2),
				"Swaps":        fmt.Sprintf("1.9"),
				"Commission":   fmt.Sprintf("%f", 0.2),
				"Taxes":        fmt.Sprintf("1.3"),

				"OpenTime": fmt.Sprintf("2017-06-07"),
			}

			fields := map[string]interface{}{
				"VOLUME":     rand.Int() % 10,
				"OpenPrice":  8.23,
				"ClosePrice": 8.55,
				"Profit":     0.32,
				"SL":         2,
				"TP":         1,
			}

			pt, err := client.NewPoint("T_Trades", tags, fields, time.Now())
			if err != nil {
				logs.Error("Error: ", err)
			}
			bp.AddPoint(pt)

		}

		err := clnt.Write(bp)
		if err != nil {
			logs.Error(err)
			continue

		}
		fmt.Printf("task %d finished:%d/%d\n", task, index, num)

	}

	fmt.Printf("%d task done\n", task)
}

func read(num int) {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://goxbtc.com:8086",
		Username: username,
		Password: password,
	})

	if err != nil {
		logs.Error("Error: ", err)
	}

	q := ""
	if 0 == num {
		q = fmt.Sprintf("select sum(TP) from T_Trades;")

	} else {
		q = fmt.Sprintf("select * from T_Trades limit %d;", num)
	}

	res, err := queryDB(c, q)
	if err != nil {
		logs.Error(err)
		return
	}

	if 0 == num {
		logs.Debug("total records:", res[0].Series[0].Values[0][1])
	} else {

		fmt.Printf("%s\n", res[0].Series[0].Columns)
		for i := 0; i < 2; i++ {
			fmt.Printf("%s\n", res[0].Series[0].Values[i])
		}
		count := len(res[0].Series[0].Values)
		logs.Debug("Found a total of %v records\n", count)
	}
}

func write(num int) {

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://goxbtc.com:8086",
		Username: username,
		Password: password,
	})

	if err != nil {
		logs.Error("Error: ", err)
	}

	record := num
	task := 1

	i := 1
	for i < task && record > 10000 {
		go writePoints(c, i, record/task)
		//fmt.Printf("i=%d\n",i)
		i += 1
	}

	writePoints(c, i, record/task)
	//fmt.Printf("task done : i=%d \n",i)
}

func main() {

	arg_num := len(os.Args)
	records := 0
	if arg_num > 1 && os.Args[1] == "-r" {

		records = 0
		if arg_num > 2 {
			record, err := strconv.Atoi(os.Args[2])
			if err != nil {
				logs.Error("wrong param:", err)
			} else {
				records = record
			}
			logs.Debug("read %d from influxdb", records)
			read(records)
		} else {
			read(0)
		}

	} else if arg_num > 1 && os.Args[1] == "-w" {

		records = 1
		if arg_num > 2 {
			record, err := strconv.Atoi(os.Args[2])
			if err != nil {
				logs.Error("wrong param:", err)
			} else {
				records = record
			}

		}

		logs.Debug("insert to db %d records", records)
		write(records)
	}

	return
}
