#!/bin/bash

# TODO: this doesn't work very well all in one terminal
#
# indicates if the network should start or just generate config files
START=false

# 
while [ "$1" != "" ]; do
    case $1 in
        --start ) shift
                START=true
                ;;
        * )     echo "Invalid option: $1"
                exit 1
    esac
    shift
done

VALIDATORS=("validator-1" "validator-2" "validator-3" "validator-4")
ADDRESSES=("0x1111111111111111111111111111111111111111" "0x2222222222222222222222222222222222222222" "0x3333333333333333333333333333333333333333" "0x4444444444444444444444444444444444444444")

# variables for pulling
LOCAL_PATH="./genesis/validators/"
REMOTE_PATH="/home/share/validators/*"

# root path for all validators
ROOTDIR="./local-validators"
GENESISDIR="genesis"
VALIDATORSDIR="${GENESISDIR}/validators"
SHARED_GENESISDIR="${ROOTDIR}/${VALIDATORSDIR}"
COMMITTEE_PATH="${ROOTDIR}/${GENESISDIR}/committee.yaml"
WORKER_CACHE_PATH="${ROOTDIR}/${GENESISDIR}/worker_cache.yaml"

# make local directory for all validators
mkdir -p $SHARED_GENESISDIR

# number of validators
LENGTH="${#VALIDATORS[@]}"

# for validator in "${VALIDATORS[@]}"; do
for ((i=0; i<$LENGTH; i++)); do
    VALIDATOR="${VALIDATORS[$i]}"
    ADDRESS="${ADDRESSES[$i]}"
    DATADIR="${ROOTDIR}/${VALIDATOR}"

    if [ -d "${DATADIR}" ]; then
        echo "${DATADIR} already exists -- skipping"
        continue
    fi

    echo "creating datadir for ${VALIDATOR}"
    cargo run --bin telcoin-network -- genesis init --datadir "${DATADIR}"

    echo "creating validator keys"
    cargo run --bin telcoin-network -- keytool generate validator --datadir "${DATADIR}" --address "${ADDRESS}"

    echo "creating validator info for genesis"
    cargo run --bin telcoin-network -- genesis add-validator --datadir "${DATADIR}"

    # cp validator info into shared genesis dir
    echo "copying validator info to shared genesis dir"
    ls "${DATADIR}/${VALIDATORSDIR}"
    cp "${DATADIR}/${VALIDATORSDIR}"/* "${SHARED_GENESISDIR}"
    echo ""
    echo ""
done

# create committee and worker cache yamls
cargo run --bin telcoin-network -- genesis create-committee --datadir "${ROOTDIR}"

for ((i=0; i<$LENGTH; i++)); do
    VALIDATOR="${VALIDATORS[$i]}"
    DATADIR="${ROOTDIR}/${VALIDATOR}"
    INSTANCE=$((i+1))

    # copy files
    cp "${COMMITTEE_PATH}" "${DATADIR}/genesis"
    cp "${WORKER_CACHE_PATH}" "${DATADIR}/genesis"

    if [ "$START" = true ]; then
        # start validator
        cargo run --bin telcoin-network -- node --datadir "${DATADIR}" --dev --chain yukon --instance "${INSTANCE}"
    fi
done
