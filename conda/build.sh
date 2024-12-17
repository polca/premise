#!/bin/bash
if [[ $PKG_NAME == "premise-bw25" ]]; then
    python -m pip install --no-deps --ignore-installed .[bw25]
elif [[ $PKG_NAME == "premise-bw2" ]]; then
    python -m pip install --no-deps --ignore-installed .[bw2]
else
    python -m pip install --no-deps --ignore-installed .
fi
