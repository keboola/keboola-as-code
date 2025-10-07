#!/bin/bash

# Test script to verify gzip compression on templates API
# This script tests if the API responds with gzip compression when Accept-Encoding: gzip is sent

echo "Testing gzip compression on Templates API..."

# Test 1: Request with Accept-Encoding: gzip
echo "Test 1: Requesting with Accept-Encoding: gzip"
response1=$(curl -s -H "Accept-Encoding: gzip" -H "Accept: application/json" \
  -w "HTTP_CODE:%{http_code}\nSIZE_DOWNLOAD:%{size_download}\n" \
  -D /tmp/headers1.txt \
  -o /tmp/response1.bin \
  http://localhost:8000/v1)

echo "Response 1:"
echo "$response1"
echo "Headers:"
grep -i "content-encoding" /tmp/headers1.txt || echo "No Content-Encoding header found"

# Check if response was compressed and decompress if needed
if grep -qi "content-encoding: gzip" /tmp/headers1.txt; then
    echo "Content was gzipped. Decompressed content:"
    cat /tmp/response1.bin | gunzip
else
    echo "Content was not compressed:"
    cat /tmp/response1.bin
fi

# Test 2: Request without Accept-Encoding: gzip
echo -e "\nTest 2: Requesting without Accept-Encoding: gzip"
response2=$(curl -s -H "Accept: application/json" \
  -w "HTTP_CODE:%{http_code}\nSIZE_DOWNLOAD:%{size_download}\n" \
  -D /tmp/headers2.txt \
  -o /tmp/response2.txt \
  http://localhost:8000/v1)

echo "Response 2:"
echo "$response2"
echo "Headers:"
grep -i "content-encoding" /tmp/headers2.txt || echo "No Content-Encoding header found"
echo "Content:"
cat /tmp/response2.txt

# Test 3: Test a specific endpoint that returns JSON
echo -e "\nTest 3: Testing repositories endpoint with gzip"
response3=$(curl -s -H "Accept-Encoding: gzip" -H "Accept: application/json" -H "X-StorageAPI-Token: $KBC_STORAGE_API_TOKEN" \
  -w "HTTP_CODE:%{http_code}\nSIZE_DOWNLOAD:%{size_download}\n" \
  -D /tmp/headers3.txt \
  -o /tmp/response3.bin \
  http://localhost:8000/v1/repositories)

echo "Response 3:"
echo "$response3"
echo "Headers:"
grep -i "content-encoding" /tmp/headers3.txt || echo "No Content-Encoding header found"

# Check if response was compressed and decompress if needed
if grep -qi "content-encoding: gzip" /tmp/headers3.txt; then
    echo "Content was gzipped. Decompressed content:"
    cat /tmp/response3.bin | gunzip
else
    echo "Content was not compressed:"
    cat /tmp/response3.bin
fi

echo -e "\nTest 4: Templates repo keboola/templates with provided headers"
response4=$(curl -s \
  -H 'accept: */*' \
  -H 'accept-language: en-US,en;q=0.9' \
  -H 'origin: https://connection.europe-west3.gcp.keboola.com' \
  -H 'priority: u=1, i' \
  -H 'referer: https://connection.europe-west3.gcp.keboola.com/' \
  -H 'sec-ch-ua: "Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Linux"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: cors' \
  -H 'sec-fetch-site: same-site' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36' \
  -H "x-storageapi-token: $KBC_STORAGE_API_TOKEN" \
  -w "HTTP_CODE:%{http_code}\nSIZE_DOWNLOAD:%{size_download}\n" \
  -D /tmp/headers4.txt \
  -o /tmp/response4.bin \
  http://localhost:8000/v1/repositories/keboola/templates)

echo "Response 4:"
echo "$response4"
echo "Headers:"
grep -i "content-encoding" /tmp/headers4.txt || echo "No Content-Encoding header found"

if grep -qi "content-encoding: gzip" /tmp/headers4.txt; then
    echo "Content was gzipped. Decompressed content:"
    cat /tmp/response4.bin | gunzip
else
    echo "Content was not compressed:"
    cat /tmp/response4.bin
fi

echo -e "\nTest 5: Project default instances with provided headers"
response5=$(curl -s \
  -H 'accept: */*' \
  -H 'accept-language: en-US,en;q=0.9' \
  -H 'origin: https://connection.europe-west3.gcp.keboola.com' \
  -H 'priority: u=1, i' \
  -H 'referer: https://connection.europe-west3.gcp.keboola.com/' \
  -H 'sec-ch-ua: "Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"' \
  -H 'sec-ch-ua-mobile: ?0' \
  -H 'sec-ch-ua-platform: "Linux"' \
  -H 'sec-fetch-dest: empty' \
  -H 'sec-fetch-mode: cors' \
  -H 'sec-fetch-site: same-site' \
  -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36' \
  -H "x-storageapi-token: $KBC_STORAGE_API_TOKEN" \
  -w "HTTP_CODE:%{http_code}\nSIZE_DOWNLOAD:%{size_download}\n" \
  -D /tmp/headers5.txt \
  -o /tmp/response5.bin \
  http://localhost:8000/v1/project/default/instances)

echo "Response 5:"
echo "$response5"
echo "Headers:"
grep -i "content-encoding" /tmp/headers5.txt || echo "No Content-Encoding header found"

if grep -qi "content-encoding: gzip" /tmp/headers5.txt; then
    echo "Content was gzipped. Decompressed content:"
    cat /tmp/response5.bin | gunzip
else
    echo "Content was not compressed:"
    cat /tmp/response5.bin
fi

echo -e "\nTest completed!" 
