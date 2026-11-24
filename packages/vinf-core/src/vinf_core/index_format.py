"""The single supported on-disk layout, shared by writers and readers."""

from struct import Struct

DOCS_MAGIC = b"VINFdoc1"
DOC_INDEX_MAGIC = b"VINFdix1"
LEXICON_MAGIC = b"VINFlex1"
LEX_INDEX_MAGIC = b"VINFlox1"
TEXTS_MAGIC = b"VINFtxt1"
TEXT_CRC_MAGIC = b"VINFtcr1"
POSTINGS_MAGIC = b"VINFpst1"
TERM_MAGIC = b"VINFtrm1"

U64 = Struct("<Q")
U32 = Struct("<I")
# UUID, token count, norm, metadata/text spans, metadata/text CRC32.
DOC_ROW = Struct("<16sQdQQQQII")
DOC_ROW_SIZE = DOC_ROW.size + U32.size
LEX_ROW = Struct("<QII")  # byte offset, size, payload CRC32
TERM_HEADER = Struct("<8sQQQQ")  # magic, DF, occurrences, block count, table offset
TERM_HEADER_SIZE = TERM_HEADER.size + U32.size
BLOCK_ROW = Struct("<QQQIII")  # first/last doc, offset, size, document count, CRC32
BLOCK_DOCUMENTS = 128
TEXT_CHUNK_SIZE = 65536

FILES = (
    ("documents.idx", DOC_INDEX_MAGIC),
    ("documents.bin", DOCS_MAGIC),
    ("lexicon.offsets", LEX_INDEX_MAGIC),
    ("lexicon.bin", LEXICON_MAGIC),
    ("texts.bin", TEXTS_MAGIC),
    ("texts.crc", TEXT_CRC_MAGIC),
    ("postings.bin", POSTINGS_MAGIC),
)
BLOCK_ROW_SIZE = BLOCK_ROW.size + U32.size
LEX_ROW_SIZE = LEX_ROW.size + U32.size
