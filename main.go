package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type inputFile struct {
	filepath  string
	separator string
	pretty    bool
}

func main() {
	// Showing useful information when the user enters the --help option
	flag.Usage = func() {
		fmt.Printf("Usage: %s [options] <csvFile>\nOptions:\n", os.Args[0])
		flag.PrintDefaults()
	}
	// Getting the file data that was entered by the user
	fileData, err := getFileData()

	if err != nil {
		exitGracefully(err)
	}
	// Validating the file entered
	if _, err := checkFileValidity(fileData.filepath); err != nil {
		exitGracefully(err)
	}
	// Declaring the channels that our go-routines are going to use
	writerChannel := make(chan map[string]string)
	done := make(chan bool)
	// Running both of our go-routines, the first one responsible for reading and the second one for writing
	go processCsvFile(fileData, writerChannel)
	go writeJSON(fileData.filepath, writerChannel, done, fileData.pretty)
	// Waiting for the done channel to receive a value, so that we can terminate the programn execution
	<-done
}

func getFileData() (inputFile, error) {
	// validate that we're getting the correct number of arguments
	if len(os.Args) < 2 {
		return inputFile{}, errors.New("a filepath argument is required")
	}

	// define option flags - name - default value - short description - help
	separator := flag.String("separator", "comma", "Column separator")
	pretty := flag.Bool("pretty", false, "Generate pretty JSON")

	flag.Parse() // parse all argumentd from the terminal

	fileLocation := flag.Arg(0) // The only argument - not a flag - makes sure the location is a CSV file

	if !(*separator == "comma" || *separator == "semicolon") {
		return inputFile{}, errors.New("only comma or semicolon seperators are allowed")
	}
	// if we get here the program arguments are validated
	// can return the corresponding struct instance with all required data
	return inputFile{fileLocation, *separator, *pretty}, nil
}

// checks if a valid csv file
func checkFileValidity(filename string) (bool, error) {
	// check if entered file is a CSV
	if fileExtension := filepath.Ext(filename); fileExtension != ".csv" {
		return false, fmt.Errorf("file %s is not CSV", filename)
	}

	// check if filepath entered belongs to an existing file
	if _, err := os.Stat(filename); err != nil && os.IsNotExist(err) {
		return false, fmt.Errorf("file %s does not exist", filename)
	}
	// if this code is reached then its a valid file
	return true, nil
}

// read the csv file
func processCsvFile(fileData inputFile, writerChannel chan<- map[string]string) {
	// open file for reading
	file, err := os.Open(fileData.filepath)
	// check for errors
	check(err)
	defer file.Close()

	// defining "headers", "line", and slice
	var headers, line []string
	// initialise CSV reader
	reader := csv.NewReader(file)
	// change between separator (,) or (;)
	if fileData.separator == "semicolon" {
		reader.Comma = ';'
	}
	// read the first line to find the headers
	headers, err = reader.Read()
	check(err)

	for {
		// read one row (line) from the csv - this line is a string slice w/ each element = a column
		line, err = reader.Read()
		//if end of file - close the channel - break from loop
		if err == io.EOF {
			close(writerChannel)
			break
		} else if err != nil {
			exitGracefully(err) // if reached - there is an unexpected error
		}
		// process a csv line
		record, err := processLine(headers, line)

		// if reached - wrong number of columns - skip line
		if err != nil {
			fmt.Printf("Line: %sError: %s\n", line, err)
			continue
		}
		// otehrwise - send the processed record to the sriter channel
		writerChannel <- record
	}
}

// write the JSON file from CSV
func writeJSON(csvPath string, writerChannel <-chan map[string]string, done chan<- bool, pretty bool) {
	// instanciate a JSON writer function
	writeString := createStringWriter(csvPath)
	// instanciate JSON parse function and break line character
	jsonFunc, breakLine := getJSONFunc(pretty)
	// log for information
	fmt.Println("Writing JSON file...")
	// writing the first char of JSON
	writeString("["+breakLine, false)

	first := true

	for {
		// waiting for pushed records into writeChannel
		record, more := <-writerChannel
		if more {
			if !first {
				// if not first break the line
				writeString(","+breakLine, false)
			} else {
				// its the first line - dont break it
				first = false
			}
			// parse the record into JSON
			jsonData := jsonFunc(record)
			// write the JSON data with writer function
			writeString(jsonData, false)
		} else {
			// if it got here - there arent anymore records to pass - close the file
			// write the final chars and close file
			writeString(breakLine+"]", true)
			fmt.Println("Complete!") // log that it's done
			done <- true             // send the signal to the main func so it can correclty exit out
			break                    // stop the loop
		}
	}
}

// terminate the program us something unexpected happens
func exitGracefully(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}

// check for errors
func check(e error) {
	if e != nil {
		exitGracefully(e)
	}
}

// takes headers and line slice to create a string map from them
func processLine(headers []string, dataList []string) (map[string]string, error) {
	// validate the same number of headers and columns - otherwise error
	if len(dataList) != len(headers) {
		return nil, errors.New("line doesn't match headers format. Skipping")
	}
	// create the map to populate
	recordMap := make(map[string]string)
	// for each header - set a new map key w/ corresponding col calue
	for i, name := range headers {
		recordMap[name] = dataList[i]
	}
	// return map
	return recordMap, nil
}

// instantiates a JSON file writer
func createStringWriter(csvPath string) func(string, bool) {
	jsonDir := filepath.Dir(csvPath)                                                       // Getting the directory where the CSV file is
	jsonName := fmt.Sprintf("%s.json", strings.TrimSuffix(filepath.Base(csvPath), ".csv")) // Declaring the JSON filename, using the CSV file name as base
	finalLocation := filepath.Join(jsonDir, jsonName)                                      // Declaring the JSON file location, using the previous variables as base
	// Opening the JSON file
	f, err := os.Create(finalLocation)
	check(err)
	// This is the function we want to return-  to write the JSON file
	return func(data string, close bool) { // 2 arguments: The piece of text to write, and whether or not should close the file
		_, err := f.WriteString(data) // Writing the data string into the file
		check(err)
		// If close is "true", it means there are no more data left to be written, so we close the file
		if close {
			f.Close()
		}
	}
}

// function to ensure JSON file is being generated with correct formatting
func getJSONFunc(pretty bool) (func(map[string]string) string, string) {
	// Declaring the variables we're going to return at the end
	var jsonFunc func(map[string]string) string
	var breakLine string
	if pretty { //Pretty is enabled, so we should return a well-formatted JSON file (multi-line)
		breakLine = "\n"
		jsonFunc = func(record map[string]string) string {
			jsonData, _ := json.MarshalIndent(record, "   ", "   ") // By doing this we're ensuring the JSON generated is indented and multi-line
			return "   " + string(jsonData)                         // Transforming from binary data to string and adding the indent characets to the front
		}
	} else { // Now pretty is disabled so we should return a compact JSON file (one single line)
		breakLine = "" // It's an empty string because we never break lines when adding a new JSON object
		jsonFunc = func(record map[string]string) string {
			jsonData, _ := json.Marshal(record) // Now we're using the standard Marshal function, which generates JSON without formating
			return string(jsonData)             // Transforming from binary data to string
		}
	}

	return jsonFunc, breakLine // Returning everythinbg
}
