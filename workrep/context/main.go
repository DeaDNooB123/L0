package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"log"
	"os"
	"strings"
)

const (
	path    = `C:\workrep\notes\`
	newPath = `C:\workrep\newnotes\`
)

type Note struct {
	HumQuery   string
	NormQuery  string
	Miner      string
	MinersArgs string
	PresArgs   string
	ShardKey   string
}

func (n Note) stringsNote() string {
	strArr := make([]string, 0)
	strArr = append(strArr, n.HumQuery, n.NormQuery, n.Miner, n.MinersArgs, n.PresArgs, n.ShardKey)
	note := strings.Join(strArr, "|")
	return note
}

func main() {
	filesNames := getFilesNames(path)

	data, e := os.Open("data.txt")
	if e != nil {
		log.Fatal(e)
	}
	defer data.Close()
	scData := bufio.NewScanner(data)
	conInfoMap := make(map[string]string, 0)
	for scData.Scan() {
		context := strings.Split(scData.Text(), " - ")[0]
		note := strings.Split(scData.Text(), " - ")[1]
		nq := strings.Split(note, "|")[1]
		conInfoMap[nq] = context
	}

	for _, v := range filesNames {

		if v == "." {
			continue
		}

		fileName := path + v
		newFileName := newPath + v
		notes, e := os.Open(fileName)
		if e != nil {
			log.Fatal(e)
		}
		defer notes.Close()
		scNotes := bufio.NewScanner(notes)
		notesArr := make([]Note, 0)

		result, e := os.Create(newFileName)
		if e != nil {
			log.Fatal(e)
		}
		defer result.Close()

		for scNotes.Scan() {
			noteString := strings.Split(scNotes.Text(), "|")

			note := &Note{}

			note.HumQuery = noteString[0]
			note.NormQuery = noteString[1]
			note.Miner = noteString[2]
			note.MinersArgs = noteString[3]
			note.PresArgs = noteString[4]
			note.ShardKey = noteString[5]

			notesArr = append(notesArr, *note)
		}

		for _, note := range notesArr {
			if val, ok := conInfoMap[note.NormQuery]; ok {
				note.Miner = "context"
				var mArgs string
				if strings.Contains(note.MinersArgs, "@sort: --apply-self-ranker=false") && !strings.Contains(note.HumQuery, "духи") {
					mArgs = fmt.Sprintf("--context-subject=\"%v\" --query=\"%v\" @sort: --apply-self-ranker=false", val, note.HumQuery)
				} else {
					mArgs = fmt.Sprintf("--context-subject=\"%v\" --query=\"%v\"", val, note.HumQuery)
				}
				note.MinersArgs = mArgs
			}
			result.WriteString(note.stringsNote() + "\n")
		}

	}

}

func getFilesNames(root string) []string {
	filesNames := make([]string, 0)
	fileSystem := os.DirFS(root)
	fs.WalkDir(fileSystem, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		filesNames = append(filesNames, path)
		return nil
	})

	return filesNames
}
