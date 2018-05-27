package main

import (
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"fmt"
	"strings"
	"time"
	"sync"
	"C"
)

//
//func get_stock(col mgo.Collation) {
//	col.Find(bson.M{})
//}
type Codes []string

type Stocks []string

type  Bar struct{
	Id	bson.ObjectId `bson:"_id,omitempty"`
	Code string
	Time time.Time
	Open float64
	Close float64
	Low float64
	High float64
	Volume int64
	Amount int64

}

func GetStocks(col *mgo.Collection) Stocks{

	codes := Codes{}
	col.Find(bson.M{}).Distinct("code", &codes)

	stocks :=  Stocks{}

	for _, code := range codes{
		if strings.HasPrefix(code, "SH60"){
			stocks = append(stocks, code)
		}

		if strings.HasPrefix(code, "SZ00"){
			stocks = append(stocks, code)
		}

		if strings.HasPrefix(code, "SZ30"){
			stocks = append(stocks, code)
		}


	}
	return stocks

}

func update_limit(stock string, sess *mgo.Session, group *sync.WaitGroup){
	sess_ := sess.Copy()
	defer sess_.Close()
	defer group.Done()

	col := sess_.DB("stock").C("kline_day")

	query := col.Find(bson.M{"code": stock}).Sort("time")
	iter := query.Iter()
	bar := Bar{}
	before := Bar{}
	for iter.Next(&bar){
		if before.Code == ""{
			col.Update(bson.M{"_id":bar.Id}, bson.M{"$set":bson.M{"limit_up":0, "limit_down":0}})
			before = bar
		}else {
			limit_up := before.Close* 1.1
			limit_down := before.Close*0.9
			err := col.Update(bson.M{"_id":bar.Id}, bson.M{"$set":bson.M{"limit_up":limit_up, "limit_down":limit_down}})
			if err != nil{
				fmt.Println(err)
			}
			before = bar
		}
	}
	fmt.Println("stock %s update  finished" ,stock)
}


func main(){
	wg :=	sync.WaitGroup{}

	session, err := mgo.Dial("localhost")
	if err != nil{
		panic(err)
	}

	defer session.Close()

	session.SetMode(mgo.Monotonic, true)
	session.SetPoolLimit(150)

	c := session.DB("stock").C("kline_day")

	stocks := GetStocks(c)

	for _, stock := range stocks{
		wg.Add(1)
		go update_limit(stock, session, &wg)
		fmt.Println(stock)
	}

	wg.Wait()
}