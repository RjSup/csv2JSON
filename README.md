# CLI Tool for converting csv to JSON

go run main.go "csv file path"

go build main.go

usage:
 ./main "csv file"
./main --pretty "csv file"

enabling pretty for a more readable JSON format: --pretty
works for csv files with both ";" and ","
