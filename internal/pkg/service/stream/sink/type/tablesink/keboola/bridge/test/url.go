package test

import (
	"strconv"

	"github.com/keboola/keboola-sdk-go/v2/pkg/keboola"
	"github.com/umisama/go-regexpcache"

	"github.com/keboola/keboola-as-code/internal/pkg/utils/errors"
)

func extractBranchIDFromURL(url string) (keboola.BranchID, error) {
	matches := regexpcache.MustCompile(`/branch/([0-9]+)(/|$)`).FindStringSubmatch(url)

	if len(matches) == 0 {
		return 0, errors.Errorf(`branchID not found in url "%s"`, url)
	}

	branchID, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, errors.Errorf(`cannot parse branchID from url "%s"`, url)
	}

	return keboola.BranchID(branchID), nil
}

func extractBucketIDFromURL(url string) (keboola.BucketID, error) {
	matches := regexpcache.MustCompile(`/buckets/([a-z0-9\.\-]+)(/|$)`).FindStringSubmatch(url)

	if len(matches) == 0 {
		return keboola.BucketID{}, errors.Errorf(`bucketID not found in url "%s"`, url)
	}

	return keboola.ParseBucketID(matches[1])
}

func extractTableIDFromURL(url string) (keboola.TableID, error) {
	matches := regexpcache.MustCompile(`/tables/([a-z0-9\.\-]+)(/|$)`).FindStringSubmatch(url)

	if len(matches) == 0 {
		return keboola.TableID{}, errors.Errorf(`tableID not found in url "%s"`, url)
	}

	return keboola.ParseTableID(matches[1])
}

func extractTokenIDFromURL(url string) (string, error) {
	matches := regexpcache.MustCompile(`/tokens/([0-9]+)(/|$)`).FindStringSubmatch(url)

	if len(matches) == 0 {
		return "", errors.Errorf(`tokenID not found in url "%s"`, url)
	}

	return matches[1], nil
}
