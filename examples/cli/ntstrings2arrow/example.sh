#!/bin/sh

inputdat=./sample-input-dat.dat

geninput(){
	test -f "${inputdat}" && return

	echo creating input data...

	touch "${inputdat}"
	truncate -s 0 "${inputdat}"

	printf 'helo\0\0\0\0' >> "${inputdat}"
	printf 'wrld\0\0\0\0' >> "${inputdat}"
	printf 'wrld\0\0\0\0' >> "${inputdat}"
	printf 'hwhw\0\0\0\0' >> "${inputdat}"
	printf 'helo\0\0\0\0' >> "${inputdat}"
}

geninput

export NTS_NAME="${inputdat}"
export CHUNK_SIZE=8

./ntstrings2arrow
