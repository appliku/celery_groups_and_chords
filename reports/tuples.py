from collections import namedtuple

REPORT_STATUS = namedtuple('REPORT_STATUS', 'processing error finished')._make(range(3))
