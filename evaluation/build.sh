echo "Building server..."
GOOS=linux GOARCH=amd64 go build ../server/
echo "Building client..."
GOOS=linux GOARCH=amd64 go build ../client/
echo "Successfully built binaries"
