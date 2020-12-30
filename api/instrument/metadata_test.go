package instrument

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGetMetadata_happyPath(t *testing.T) {
	m := GetMetadata("EURUSD")
	assert.NotNil(t, m)
	assert.Equal(t, "eurusd", m.Code())
	assert.Equal(t, "EUR/USD", m.Name())
	assert.Equal(t, "Euro vs US Dollar", m.Description())
	assert.Equal(t, float64(100000), m.DecimalFactor())
}

func TestGetMetadata_blank(t *testing.T) {
	m := GetMetadata("NOT_EXISTENT")
	assert.Nil(t, m)
}

func TestMetadata_PriceToString(t *testing.T) {
	m := GetMetadata("EURUSD")
	assert.Equal(t, "1.22464", m.PriceToString(1.22464))
	assert.Equal(t, "1.00000", m.PriceToString(1))
	assert.Equal(t, "1.01000", m.PriceToString(1.01))
}
