package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	url = "http://popularity-service.wbx-analytics.svc.k8s.wbxsearch-dl/popularity?"
)

type Val struct {
	Value float64 `json:"value"`
}

func main() {

	file, e := os.Open("data.txt")
	if e != nil {
		log.Fatal(e)
	}
	defer file.Close()

	newfile, e := os.Create("popularity_more_ten.txt")
	if e != nil {
		log.Fatal(e)
	}
	defer newfile.Close()

	newfile2, e := os.Create("popularity_less_ten.txt")
	if e != nil {
		log.Fatal(e)
	}
	defer newfile2.Close()

	sc := bufio.NewScanner(file)

	for sc.Scan() {
		pres := strings.Split(sc.Text(), "|")[4]
		var numOfPres string
		if strings.Contains(pres, "&") {
			numOfPres = strings.Split(pres, "&")[0]
		} else {
			numOfPres = pres
		}

		fullUrl := url + numOfPres
		resp, e := http.Get(fullUrl)
		if e != nil {
			log.Println(e)

		}
		if resp.Status != "200 OK" {
			fmt.Println(resp.Status)
			fmt.Println(numOfPres)
		}
		fmt.Println(resp.Status)

		body, e := io.ReadAll(resp.Body)
		if e != nil {
			log.Fatal(e)
		}

		defer resp.Body.Close()

		var val Val

		json.Unmarshal(body, &val)

		fmt.Println(val.Value)

		if val.Value >= 10.00 {
			newfile.WriteString(string(body) + " --- " + sc.Text() + "\n")
		} else {
			newfile2.WriteString(string(body) + " --- " + sc.Text() + "\n")
		}
		time.Sleep(25 * time.Millisecond)
	}
}
