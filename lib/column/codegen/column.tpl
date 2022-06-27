// Licensed to ClickHouse, Inc. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. ClickHouse, Inc. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Code generated by make codegen DO NOT EDIT.
// source: lib/column/codegen/column.tpl

package column

import (
	"math/big"
	"reflect"
	"strings"
	"fmt"
	"time"
	"net"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
	"github.com/shopspring/decimal"
)

func (t Type) Column(name string) (Interface, error) {
	switch t {
{{- range . }}
	case "{{ .ChType }}":
		return &{{ .ChType }}{name: name}, nil
{{- end }}
	case "Int128":
		return &BigInt{
			size: 16,
			chType: t,
			name: name,
			signed: true,
		}, nil
	case "UInt128":
		return &BigInt{
			size: 16,
			chType: t,
			name: name,
			signed: false,
		}, nil
	case "Int256":
		return &BigInt{
			size: 32,
			chType: t,
			name: name,
			signed: true,
		}, nil
	case "UInt256":
		return &BigInt{
			size: 32,
			chType: t,
			name: name,
			signed: false,
		}, nil
	case "IPv4":
		return &IPv4{name: name}, nil
	case "IPv6":
		return &IPv6{name: name}, nil
	case "Bool", "Boolean":
		return &Bool{name: name}, nil
	case "Date":
		return &Date{name: name}, nil
	case "Date32":
		return &Date32{name: name}, nil
	case "UUID":
		return &UUID{name: name}, nil
	case "Nothing":
		return &Nothing{name: name}, nil
	case "Ring":
		set, err := (&Array{name: name}).parse("Array(Point)")
        if err != nil {
            return nil, err
        }
        set.chType = "Ring"
        return &Ring{
            set:  set,
            name: name,
        }, nil
	case "Polygon":
		set, err := (&Array{name: name}).parse("Array(Ring)")
        if err != nil {
            return nil, err
        }
        set.chType = "Polygon"
        return &Polygon{
            set:  set,
            name: name,
        }, nil
	case "MultiPolygon":
		set, err := (&Array{name: name}).parse("Array(Polygon)")
        if err != nil {
            return nil, err
        }
        set.chType = "MultiPolygon"
        return &MultiPolygon{
            set:  set,
            name: name,
        }, nil
	case "Point":
		return &Point{name: name}, nil
	case "String":
		return &String{name: name}, nil
	case "Object('json')":
	    return &JSONObject{name: name, root: true}, nil
	}

	switch strType := string(t); {
	case strings.HasPrefix(string(t), "Map("):
		return (&Map{name: name}).parse(t)
	case strings.HasPrefix(string(t), "Tuple("):
		return (&Tuple{name: name}).parse(t)
	case strings.HasPrefix(string(t), "Decimal("):
		return (&Decimal{name: name}).parse(t)
	case strings.HasPrefix(strType, "Nested("):
		return (&Nested{name: name}).parse(t)
	case strings.HasPrefix(string(t), "Array("):
		return (&Array{name: name}).parse(t)
	case strings.HasPrefix(string(t), "Interval"):
		return (&Interval{name: name}).parse(t)
	case strings.HasPrefix(string(t), "Nullable"):
		return (&Nullable{name: name}).parse(t)
	case strings.HasPrefix(string(t), "FixedString"):
		return (&FixedString{name: name}).parse(t)
	case strings.HasPrefix(string(t), "LowCardinality"):
		return (&LowCardinality{name: name}).parse(t)
	case strings.HasPrefix(string(t), "SimpleAggregateFunction"):
		return (&SimpleAggregateFunction{name: name}).parse(t)
	case strings.HasPrefix(string(t), "Enum8") || strings.HasPrefix(string(t), "Enum16"):
		return Enum(t, name)
	case strings.HasPrefix(string(t), "DateTime64"):
		return (&DateTime64{name: name}).parse(t)
	case strings.HasPrefix(strType, "DateTime") && !strings.HasPrefix(strType, "DateTime64"):
		return (&DateTime{name: name}).parse(t)
	}
	return nil, &UnsupportedColumnTypeError{
		t: t,
	}
}

type (
{{- range . }}
	{{ .ChType }} struct {
	    data []{{ .GoType }}
	    name string
	}
{{- end }}
)

var (
{{- range . }}
	_ Interface = (*{{ .ChType }})(nil)
{{- end }}
)

var (
	{{- range . }}
		scanType{{ .ChType }} = reflect.TypeOf({{ .GoType }}(0))
	{{- end }}
		scanTypeIP      = reflect.TypeOf(net.IP{})
		scanTypeBool    = reflect.TypeOf(true)
		scanTypeByte    = reflect.TypeOf([]byte{})
		scanTypeUUID    = reflect.TypeOf(uuid.UUID{})
		scanTypeTime    = reflect.TypeOf(time.Time{})
		scanTypeRing    = reflect.TypeOf(orb.Ring{})
		scanTypePoint   = reflect.TypeOf(orb.Point{})
		scanTypeSlice   = reflect.TypeOf([]interface{}{})
		scanTypeMap	    = reflect.TypeOf(map[string]interface{}{})
		scanTypeBigInt  = reflect.TypeOf(&big.Int{})
		scanTypeString  = reflect.TypeOf("")
		scanTypePolygon = reflect.TypeOf(orb.Polygon{})
		scanTypeDecimal = reflect.TypeOf(decimal.Decimal{})
		scanTypeMultiPolygon = reflect.TypeOf(orb.MultiPolygon{})
	)

{{- range . }}

func (col *{{ .ChType }}) Name() string {
	return col.name
}

func (col *{{ .ChType }}) Type() Type {
	return "{{ .ChType }}"
}

func (col *{{ .ChType }}) ScanType() reflect.Type {
	return scanType{{ .ChType }}
}

func (col *{{ .ChType }}) Rows() int {
	return len(col.data)
}

func (col *{{ .ChType }}) ScanRow(dest interface{}, row int) error {
	value := *col
	switch d := dest.(type) {
	case *{{ .GoType }}:
		*d = value.data[row]
	case **{{ .GoType }}:
		*d = new({{ .GoType }})
		**d = value.data[row]
	{{- if eq .ChType "Int64" }}
	case *time.Duration:
		*d = time.Duration(value.data[row])
	{{- end }}
	default:
		return &ColumnConverterError{
			Op:   "ScanRow",
			To:   fmt.Sprintf("%T", dest),
			From: "{{ .ChType }}",
			Hint: fmt.Sprintf("try using *%s", scanType{{ .ChType }}),
		}
	}
	return nil
}

func (col *{{ .ChType }}) Row(i int, ptr bool) interface{} {
	value := *col
	if ptr {
		return &value.data[i]
	}
	return value.data[i]
}

func (col *{{ .ChType }}) Append(v interface{}) (nulls []uint8,err error) {
	switch v := v.(type) {
	case []{{ .GoType }}:
		col.data, nulls = append(col.data, v...), make([]uint8, len(v))
	case []*{{ .GoType }}:
		nulls = make([]uint8, len(v))
		for i, v:= range v {
			switch {
			case v != nil:
				col.data = append(col.data, *v)
			default:
				col.data, nulls[i] = append(col.data, 0), 1
			}
		}
	default:
		return nil, &ColumnConverterError{
			Op:   "Append",
			To:   "{{ .ChType }}",
			From: fmt.Sprintf("%T", v),
		}
	}
	return
}

func (col *{{ .ChType }}) AppendRow(v interface{}) error {
	switch v := v.(type) {
	case {{ .GoType }}:
		col.data = append(col.data, v)
	case *{{ .GoType }}:
		switch {
		case v != nil:
			col.data = append(col.data, *v)
		default:
			col.data = append(col.data, 0)
		}
	case nil:
		col.data = append(col.data, 0)
	{{- if eq .ChType "UInt8" }}
	case bool:
		var t uint8
		if v {
			t = 1
		}
		col.data = append(col.data, t)
	{{- end }}
	{{- if eq .ChType "Int64" }}
    case time.Duration:
        col.data = append(col.data, int64(v))
    case *time.Duration:
        col.data = append(col.data, int64(*v))
	{{- end }}
	default:
		return &ColumnConverterError{
			Op:   "AppendRow",
			To:   "{{ .ChType }}",
			From: fmt.Sprintf("%T", v),
		}
	}
	return nil
}

{{- end }}