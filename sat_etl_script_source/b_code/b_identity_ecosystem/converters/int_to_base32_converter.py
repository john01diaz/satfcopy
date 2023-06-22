import string

BASE32_ALPHABET = \
    string.ascii_uppercase + string.digits


def convert_int_to_base32(
        int_value: int) \
        -> str:
    binary = \
        bin(
            int_value)[2:]  # Get binary representation of the integer
    
    binary = \
        binary.zfill(
        (len(
            binary) + 4) // 5 * 5)  # Pad to multiple of 5 bits
    
    return \
        ''.join(
            BASE32_ALPHABET[int(
                binary[i:i + 5],
                2)] for i in range(
                0,
                len(
                    binary),
                5))
