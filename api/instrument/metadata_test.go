package instrument

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetMetadata_happyPath(t *testing.T) {
	m := GetMetadata("EURUSD")
	assert.NotNil(t, m)
	assert.Equal(t, "EURUSD", m.Code())
	assert.Equal(t, "EUR/USD", m.Name())
	assert.Equal(t, "Euro vs US Dollar", m.Description())
	assert.Equal(t, float64(100000), m.DecimalFactor())
}

func TestGetMetadata_blank(t *testing.T) {
	m := GetMetadata("NOT_EXISTENT")
	assert.Nil(t, m)
}

func TestGetMetadataByName_happyPath(t *testing.T) {
	m := GetMetadataByName("A.US/USD")
	assert.NotNil(t, m)
	assert.Equal(t, "AUSUSD", m.Code())
	assert.Equal(t, "AGILENT TECHNOLOGIES INC", m.Description())
}

func TestGetMetadataByName_blank(t *testing.T) {
	m := GetMetadataByName("NOT_EXISTENT")
	assert.Nil(t, m)
}

func TestMetadata_PriceToString(t *testing.T) {
	m := GetMetadata("EURUSD")
	assert.Equal(t, "1.22464", m.PriceToString(1.22464))
	assert.Equal(t, "1.00000", m.PriceToString(1))
	assert.Equal(t, "1.01000", m.PriceToString(1.01))
}

func TestMetadata_DiffInPips(t *testing.T) {
	gold := GetMetadata("XAUUSD")
	diff := gold.DiffInPips("2352.68", "2354.90")
	assert.Equal(t, "222", diff, "diff")
}
