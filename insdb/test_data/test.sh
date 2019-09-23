#!/bin/bash

a="test sting"

testfun() {
    echo "$1"
}


testfun a
echo "$a" | testfun
