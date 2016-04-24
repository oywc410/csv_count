package main

import (
	"bufio"
	"io"
	"strings"
	"os"
	"time"
	"fmt"
	"strconv"
	"flag"
)

var customerFile = "customer.csv"
var orderFile = "order.csv"

var customer_id = flag.Int("customer_id", 0, "")

type monOrderCount struct {
	count int
	dateKey int
	a int
	b int
	c int
	rank int
}

type customerDate struct {
	customerId int
	orderCounts map[int]*monOrderCount
	createDateKey int
	endDateKey int
}

type customerDates struct {
	dates map[int]*customerDate
}

func (c customerDate) String() string {
	str := ""


	if len(c.orderCounts) == 0 {
		return str
	}

	for key, ordercount := range c.orderCounts {
		str += fmt.Sprintf("[%v:[count:%v,a:%v,b:%v,c:%v]]\n", key, ordercount.count, ordercount.a, ordercount.b, ordercount.c)
	}

	return str
}

func toDateKey(str string) int {
	dateYear, _ := strconv.Atoi(str[1:5])
	dateMon, _ := strconv.Atoi(str[6:8])
	return dateYear * 12 + dateMon
}

func main() {

	flag.Parse()

	fmt.Println(*customer_id)

	t1 := time.Now();

	customerDates := customerDates{}
	customerDates.dates = map[int]*customerDate{}

	nowDateKey := 2016 * 12 + 4

	ReadLine(customerFile, func(str string) {

		date := strings.Split(str, ",")
		if len(date) == 6 {
			customerId, _ := strconv.Atoi(date[0])
			createKey := toDateKey(date[2])
			endKey := 0
			if len(date[3]) > 0 {
				endKey = toDateKey(date[3])
			}

			tmpMap := map[int]*monOrderCount{}
			stopKey := nowDateKey
			startKey := createKey

			if endKey != 0 && endKey < nowDateKey {
				stopKey = endKey
			}

			for {
				tmpMap[startKey] =  &monOrderCount{
					dateKey: startKey,
					count: 0,
					a: 0,
					b: 0,
					c: 0,
				}
				startKey++
				if startKey > stopKey {
					break
				}
			}

			customerDates.dates[customerId] = &customerDate{
				customerId: customerId,
				orderCounts: tmpMap,
				createDateKey: createKey,
				endDateKey: stopKey,
			}
		}
	})
	orderCount := 0


	ReadLine(orderFile, func(str string) {
		orderCount++
		date := strings.Split(str, ",")
		if len(date) == 5 {
			customerId, _ := strconv.Atoi(date[1])
			dateKey := toDateKey(date[4])

			if _, ok := customerDates.dates[customerId]; ok {
				if monData := customerDates.dates[customerId].orderCounts[dateKey]; monData == nil {
					//  error
				} else {
					customerDates.dates[customerId].orderCounts[dateKey].count++
				}
			}
		}
	})

	for _, customer := range customerDates.dates {
		startKey := customer.createDateKey
		stopKey := customer.endDateKey
		monCount := customer.orderCounts

		a ,b ,c := 0, 0, 0

		for {
			a += getCount(monCount, startKey)
			a -= getCount(monCount, startKey - 12)
			b += getCount(monCount, startKey - 12)
			b -= getCount(monCount, startKey - 24)
			c += getCount(monCount, startKey - 24)
			c -= getCount(monCount, startKey - 36)

			monCount[startKey].a = a
			monCount[startKey].b = b
			monCount[startKey].c = c

			startKey++
			if startKey > stopKey {
				break
			}
		}
	}


	fmt.Println(customerDates.dates[*customer_id])
	fmt.Println(len(customerDates.dates))
	fmt.Println(orderCount)
	fmt.Println(time.Now().Sub(t1))
}

func getCount(dateMap map[int]*monOrderCount, dateKey int) int {
	if len(dateMap) != 0 {
		if _, ok := dateMap[dateKey]; ok {
			return dateMap[dateKey].count
		}
		return 0
	}

	return 0
}

func ReadLine(fileName string, handler func(string)) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	buf := bufio.NewReader(f)
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		handler(line)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}