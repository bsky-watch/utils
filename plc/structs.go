package plc

//go:generate go run ./gen

type Op struct {
	Type                string             `json:"type" cborgen:"type,const=plc_operation"`
	RotationKeys        []string           `json:"rotationKeys" cborgen:"rotationKeys"`
	VerificationMethods map[string]string  `json:"verificationMethods" cborgen:"verificationMethods"`
	AlsoKnownAs         []string           `json:"alsoKnownAs" cborgen:"alsoKnownAs"`
	Services            map[string]Service `json:"services" cborgen:"services"`
	Prev                *string            `json:"prev" cborgen:"prev"`
	Sig                 *string            `json:"sig" cborgen:"sig,omitempty"`
}

type Service struct {
	Type     string `json:"type" cborgen:"type"`
	Endpoint string `json:"endpoint" cborgen:"endpoint"`
}

type Tombstone struct {
	Type string  `json:"type" cborgen:"type,const=plc_tombstone"`
	Prev string  `json:"prev" cborgen:"prev"`
	Sig  *string `json:"sig" cborgen:"sig,omitempty"`
}
