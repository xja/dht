package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/shiyanhui/dht"
	"net/http"
	_ "net/http/pprof"
	"database/sql"
    _ "github.com/ziutek/mymysql/godrv"
	//"strings"
	"strconv"
	"time"
	"log"
	"os"
)

type file struct {
	Path   []interface{} `json:"path"`
	Length int           `json:"length"`
}

type bitTorrent struct {
	InfoHash string `json:"infohash"`
	Name     string `json:"name"`
	Files    []file `json:"files,omitempty"`
	Length   int    `json:"length,omitempty"`
}

func main() {
	go func() {
		http.ListenAndServe(":6060", nil)
	}()

	w := dht.NewWire(65536, 1024, 256)
	go func() {
		for resp := range w.Response() {
			metadata, err := dht.Decode(resp.MetadataInfo)
			if err != nil {
				continue
			}
			info := metadata.(map[string]interface{})

			if _, ok := info["name"]; !ok {
				continue
			}

			bt := bitTorrent{
				InfoHash: hex.EncodeToString(resp.InfoHash),
				Name:     info["name"].(string),
			}
			
			// total file size
			var total_length uint64 = 0
			if v, ok := info["files"]; ok {
				files := v.([]interface{})
				bt.Files = make([]file, len(files))
				
				for i, item := range files {
					f := item.(map[string]interface{})
					bt.Files[i] = file{
						Path:   f["path"].([]interface{}),
						Length: f["length"].(int),
					}
					total_length += uint64(f["length"].(int))
				}
			} else if _, ok := info["length"]; ok {
				bt.Length = info["length"].(int)
			}

			files_json, err := json.Marshal(bt.Files)
			if err == nil {
				//fmt.Println(bt.Files)
				con, err := sql.Open("mymysql", "magnet" + "/" + "torrent" + "/" + "ndBEwdl26Q-iuxIQ")
                if err != nil {
                        panic(err)
                    }
                    // runs before return
                //defer con.Close()
                    // check if already exists, use insert ignore instead check before inserting
                // var id = 0
                // con.QueryRow("select id from hash_table where infohash=? limit 1", bt.InfoHash).Scan( & id)
                    // _, err = con.Exec("insert into hash_table (infohash, name, length) VALUEs (\"cb6a91da42636fe9dbf1316b11621d7a642752d8\", \"Daisy_Stone.1080.mp4\", 1109711913)")
					// append if exists, otherwise create and write only
				logFile, err := os.OpenFile("spider.log", os.O_APPEND | os.O_CREATE | os.O_WRONLY, 0777)
				logger:=log.New(logFile,"\r\n", log.Ldate)
                /* if id == 0 {
					// fmt.Printf("%#v\n\n%#v\n\n%#v\n\n%#v\n\n\n",  bt.InfoHash, bt.Name, bt.Files, bt.Length)
                    if len(bt.Files) == 0 {
                        _, err := con.Exec("insert delayed into hash_table (infohash, name, length, len_h, date, file_count) values (?, ?, ?, ?, ?, ?)", bt.InfoHash, bt.Name, bt.Length, len_h_calc(uint64(bt.Length)), time.Now().Format("2006-01-02"), 1)
						if err != nil{
							logger.Printf("%s\n%s\n%s\n",err.Error() ,bt.InfoHash, bt.Name)
						}
                    } else {
						_, err := con.Exec("insert delayed into hash_table (infohash, name, length, files, file_count, len_h, date) values (?, ?, ?, ?, ?, ?, ?)", bt.InfoHash, bt.Name, total_length, files_json, len(bt.Files), len_h_calc(total_length), time.Now().Format("2006-01-02"))
						if err != nil{
							logger.Printf("%s\n%s\n%s\n",err.Error() ,bt.InfoHash, bt.Name)
						}
                    }
					fmt.Println(bt.Name + "\n")
                } */ // end of if id == 0
				
				if len(bt.Files) == 0 {
					_, err := con.Exec("insert ignore into hash_table (infohash, name, length, len_h, date, file_count) values (?, ?, ?, ?, ?, ?)", bt.InfoHash, bt.Name, bt.Length, len_h_calc(uint64(bt.Length)), time.Now().Format("2006-01-02"), 1)
					//fmt.Println(bt.Name + "\n")
					if err != nil{
						logger.Printf("%s\n%s\n%s\n",err.Error() ,bt.InfoHash, bt.Name)
					}
				} else {
					_, err := con.Exec("insert ignore into hash_table (infohash, name, length, files, file_count, len_h, date) values (?, ?, ?, ?, ?, ?, ?)", bt.InfoHash, bt.Name, total_length, files_json, len(bt.Files), len_h_calc(total_length), time.Now().Format("2006-01-02"))
					//fmt.Println(bt.Name + "\n")
					if err != nil{
						logger.Printf("%s\n%s\n%s\n",err.Error() ,bt.InfoHash, bt.Name)
					}
				}
				con.Close()
				logFile.Close()
			}else{
				fmt.Println(err)
			}
		}
	}()
	go w.Run()

	config := dht.NewCrawlConfig()
	config.OnAnnouncePeer = func(infoHash, ip string, port int) {
		w.Request([]byte(infoHash), ip, port)
	}
	d := dht.New(config)

	d.Run()
}

func len_h_calc(len uint64) string{
	var l float32
	s := ""
	if len < 1024 {
		s = strconv.FormatUint(len, 10) + "Bytes"
	}else{
		if (len / 1024) < 1024 {
			l = float32(len) / 1024
			s = fmt.Sprintf("%.2f", l) + "KiB"
		}else{
			if (len / 1024 / 1024) < 1024 {
				l = float32(len) / 1024 / 1024
				s = fmt.Sprintf("%.2f", l) + "MiB"
			}else{
				if (len / 1024 / 1024 / 1024) < 1024 {
					l = float32(len) / 1024 / 1024 / 1024
					s = fmt.Sprintf("%.2f", l) + "GiB"
				}else{
					l = float32(len) / 1024 / 1024 / 1024 / 1024
					s = fmt.Sprintf("%.2f", l) + "TiB"
				}
			}
		}
	}
	return s
}
