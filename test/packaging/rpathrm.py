#!/usr/bin/env python3
#  SPDX-License-Identifier: BSD-3-Clause
#  Copyright (C) 2024 Intel Corporation
#  All rights reserved.

import argparse
import shutil
import os
import struct
import sys

DT_NULL = 0
DT_RPATH = 15
DT_RUNPATH = 29

DYNSTR_SEC = 3
DYN_SEC = 6

SEC_HDR_SIZE = 64
SEC_SIZE = 16


def rb(data, s_format):
    return struct.unpack(data, s_format)[0]


def is_elf(data):
    magic = rb(">I", data[:4])
    # .ELF
    return magic == 0x7F454C46


def is_exec_64_elf(data):
    # ELFCLASS64
    return data[4] == 0x2


def is_exec_lsb_elf(data):
    # ELFDATA2LSB
    return data[5] == 0x1


def is_exec_elf(data):
    # ET_EXEC, ET_DYN
    exe = rb("I", data[16:20]) & 0xFFFF

    return exe in (0x2, 0x3)


def is_valid_bin(data):
    return (
        is_elf(data)
        and is_exec_64_elf(data)
        and is_exec_lsb_elf(data)
        and is_exec_elf(data)
    )


def rstring(data):
    _data = bytearray()
    for byte in data:
        if byte == 0:
            break
        _data.append(byte)
    return _data.decode()


def find_elf_section(section, data):
    sections = {"dynstr": DYNSTR_SEC, "dynamic": DYN_SEC}

    if section not in sections:
        return (None, None)

    e_shoff = rb("Q", data[40:48])
    e_shentsize = rb("I", data[58:62]) & 0xFFFF
    e_shnum = rb("I", data[60:64]) & 0xFFFF

    shdr = bbin[e_shoff:e_shoff + e_shentsize * e_shnum]

    while len(shdr) > 0:
        shdr_type = rb("I", shdr[4:8])
        if shdr_type == sections[section]:
            print(f"{section} section found", file=sys.stderr)
            return (rb("Q", shdr[24:32]), rb("Q", shdr[32:40]))
        # Shift 64 bytes to get the next section header
        shdr = shdr[SEC_HDR_SIZE:]
    print(f"{section} section not found", file=sys.stderr)
    return (None, None)


def mask_rpath(path, out, data):
    data = bytearray(data)

    if not is_valid_bin(data):
        print(f"{path} is not a valid ELF executable", file=sys.stderr)
        return 1

    dyn_sec_offset, dyn_sec_size = find_elf_section("dynamic", data)
    dynstr_offset, dynstr_size = find_elf_section("dynstr", data)

    if not all([dyn_sec_offset, dyn_sec_size, dynstr_offset, dynstr_size]):
        return 1

    d_tag_offset_rpath = 0
    d_tag_val_rpath = 0
    d_tag_offset_runpath = 0
    d_tag_val_runpath = 0
    offset = 0

    # Keep looping until we find DT_RPATH or DT_RUNPATH. If we find DR_RUNPATH
    # it takes precedence as DT_RPATH is already deprecated. Break when we reach
    # end of the array.

    dyn_sec = data[dyn_sec_offset:dyn_sec_offset + dyn_sec_size]
    print("Looking for rpath in dynamic section", file=sys.stderr)

    while len(dyn_sec) > 0:
        # d_tag is 8 bytes long, but we know that DTs we are looking for are
        # represented by a value within a single byte so exploit that.
        d_tag = dyn_sec[0]
        val = rb("Q", dyn_sec[8:16])
        if d_tag == DT_NULL:
            # DT_NULL - this is where we end
            break
        if d_tag == DT_RPATH:
            # DT_RPATH
            d_tag_offset_rpath = dyn_sec_offset
            d_tag_val_rpath = val
        elif d_tag == DT_RUNPATH:
            # DT_RUNPATH
            d_tag_offset_runpath = dyn_sec_offset
            d_tag_val_runpath = val
            # We can also break here
            break
        dyn_sec = dyn_sec[SEC_SIZE:]
        dyn_sec_offset += SEC_SIZE

    if d_tag_offset_runpath > 0:
        offset = d_tag_offset_runpath
        # In this context val is a string table offset
        val = d_tag_val_runpath
    elif d_tag_offset_rpath > 0:
        offset = d_tag_offset_rpath
        # In this context val is a string table offset
        val = d_tag_val_rpath
    else:
        print("rpath not found", file=sys.stderr)
        return 1

    # Get actual rpath string for verbosity
    dynstr_table = data[dynstr_offset:dynstr_offset + dynstr_size]
    dynstr = rstring(dynstr_table[val:])
    # Prepare \0 array to null the rpath string
    null = bytearray([0x0] * len(dynstr))

    count = 0
    for byte in null:
        data[dynstr_offset + val + count] = byte
        count += 1

    print(f"Offset: {offset}\nPath: {path}\nrpath: {dynstr}")

    if out:
        print(f"Hiding rpath from {path} and saving to {out}", file=sys.stderr)
        with open(out, "wb") as _out:
            _out.write(bytes(data))
        os.chmod(out, 0o0755)

    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Mask DT_R[UN]PATH from an ELF executable"
    )
    parser.add_argument("binary", type=str, help="executable to work with")
    parser.add_argument(
        "-o",
        "--output",
        dest="output",
        help="path where to save modified binary",
        required=False,
        type=str,
        default="",
    )
    args = parser.parse_args()

    bin_path = shutil.which(args.binary)
    output = args.output

    if not bin_path:
        sys.exit(1)
    with open(bin_path, "rb") as b:
        bbin = b.read()
    sys.exit(mask_rpath(bin_path, output, bbin))
