"""
dpapi.py - DPAPI access library

This file uses code originally created by Crusher Joe:
http://article.gmane.org/gmane.comp.python.ctypes/420
"""

from ctypes import *
from ctypes.wintypes import DWORD

LocalFree = windll.kernel32.LocalFree
memcpy = cdll.msvcrt.memcpy
CryptProtectData = windll.crypt32.CryptProtectData
CryptUnprotectData = windll.crypt32.CryptUnprotectData
CRYPTPROTECT_UI_FORBIDDEN = 0x01
extraEntropy = "dfgh9r8y39**/40304g0349g(G$((G$(G()$()G/()$Grejigferghzxczxc"

class DATA_BLOB(Structure):
    _fields_ = [("cbData", DWORD), ("pbData", POINTER(c_char))]

def getData(blobOut):
    cbData = int(blobOut.cbData)
    pbData = blobOut.pbData
    buffer = c_buffer(cbData)
    memcpy(buffer, pbData, cbData)
    LocalFree(pbData);
    return buffer.raw

def Win32CryptProtectData(plainText, entropy):
    bufferIn = c_buffer(plainText, len(plainText))
    blobIn = DATA_BLOB(len(plainText), bufferIn)
    bufferEntropy = c_buffer(entropy, len(entropy))
    blobEntropy = DATA_BLOB(len(entropy), bufferEntropy)
    blobOut = DATA_BLOB()

    if CryptProtectData(byref(blobIn), u"python_data", byref(blobEntropy),
                       None, None, CRYPTPROTECT_UI_FORBIDDEN, byref(blobOut)):
        return getData(blobOut)
    else:
        return ""

def Win32CryptUnprotectData(cipherText, entropy):
    bufferIn = c_buffer(cipherText, len(cipherText))
    blobIn = DATA_BLOB(len(cipherText), bufferIn)
    bufferEntropy = c_buffer(entropy, len(entropy))
    blobEntropy = DATA_BLOB(len(entropy), bufferEntropy)
    blobOut = DATA_BLOB()
    if CryptUnprotectData(byref(blobIn), None, byref(blobEntropy), None, None,
                              CRYPTPROTECT_UI_FORBIDDEN, byref(blobOut)):
        return getData(blobOut)
    else:
        return ""

def cryptData(text):
    return Win32CryptProtectData(text, extraEntropy)

def decryptData(cipher_text):
    return Win32CryptUnprotectData(cipher_text, extraEntropy)

def main():
  data = 'abc'
  print 'text to encrypt: ', data
  cipher = cryptData(data)
  plain = decryptData(cipher)
  print 'decrypted cipher: ', plain

if __name__ == '__main__':
  main()
