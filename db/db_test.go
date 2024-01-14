package db

import "testing"

func TestValueType(t *testing.T) {

	info, err := getValueTypeInfo(1)
	if err != nil {
		t.Fatal(err)
	}

	if info.valueType != ValueTypeInt {
		t.Fatalf("expected value type integer got %v", info.valueType)
	}

	info, err = getValueTypeInfo("test")
	if err != nil {
		t.Fatal(err)
	}

	if info.valueType != ValueTypeString {
		t.Fatalf("expected value type integer got %v", info.valueType)
	}

	info, err = getValueTypeInfo(1.1)
	if err != nil {
		t.Fatal(err)
	}

	if info.valueType != ValueTypeFloat {
		t.Fatalf("expected value type integer got %v", info.valueType)
	}

	info, err = getValueTypeInfo(true)
	if err != nil {
		t.Fatal(err)
	}

	if info.valueType != ValueTypeBool {
		t.Fatalf("expected value type integer got %v", info.valueType)
	}

	info, _ = getValueTypeInfo(nil)

	if info.valueType != ValueTypeUnknown {
		t.Fatalf("expected value type integer got %d", info.valueType)
	}

}
