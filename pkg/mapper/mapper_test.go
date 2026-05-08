package mapper_test

import (
    "testing"

    "github.com/diegodesousas/go-devkit/pkg/mapper"
    "github.com/stretchr/testify/assert"
)

func TestMapperSuccess(t *testing.T) {
    newMapper := mapper.NewMapper[string, int64]()

    newMapper.
        Set("t1", int64(100)).
        Set("t2", int64(200)).
        Set("t3", int64(300))

    value, err := newMapper.Get("t2")

    assert.NoError(t, err)
    assert.Equal(t, value, int64(200))
}

func TestMapperSuccess_With_Custom_Type_Key(t *testing.T) {
    type CustomKey string

    const T1 CustomKey = "t1"
    const T2 CustomKey = "t2"
    const T3 CustomKey = "t3"

    newMapper := mapper.NewMapper[CustomKey, int64]()

    newMapper.
        Set(T1, int64(100)).
        Set(T2, int64(200)).
        Set(T3, int64(300))

    value, err := newMapper.Get(T2)

    assert.NoError(t, err)
    assert.Equal(t, value, int64(200))
}

func TestMapperError(t *testing.T) {
    newMapper := mapper.NewMapper[string, int64]()

    newMapper.
        Set("t1", int64(100)).
        Set("t2", int64(200)).
        Set("t3", int64(300))

    value, err := newMapper.Get("invalid-key")

    assert.EqualError(t, err, mapper.ErrValueNotFound.Error())
    assert.Equal(t, value, int64(0))
}
