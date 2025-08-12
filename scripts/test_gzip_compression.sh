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
response3=$(curl -s -H "Accept-Encoding: gzip" -H "Accept: application/json" \
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

echo -e "\nTest completed!" 
