# Log2Kusto

log2kusto is a Python logging handler library for saving the generated log records to a Kusto database table.

## Installation

Use the package manager [pip](link will be updated) to install foobar.

```bash
pip install log2kusto
```

# Requirments
- Python logging library
- An MS internal library for providing credentials to authenticate to Kusto (working on making it open source.)

## Usage

```python
from log2kusto.kusto_handler import KustoHandler 

import logging
from log2kusto.kusto_handler import KustoHandler

class CustomFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='%', validate=True, attributes_list=[]):
        super().__init__(fmt, datefmt, style, validate)
        self.attributes = attributes_list

    def format(self, record):
        print("in format")
        for attr in self.attributes:
            print(attr)
            if not hasattr(record, attr):
                setattr(record, attr, '')
        return super().format(record)

#https://docs.python.org/3/library/logging.html#logrecord-attributes
logrecord_attributes_list = ['asctime', 'levelname', 'filename', 'funcName', 'module', 'message', 'domain']
custom_attributes_list = ['env', 'domain']
all_attributes_list = logrecord_attributes_list + custom_attributes_list
formatter = CustomFormatter('%(' + ((')s' + " ; " + '%(').join(all_attributes_list)) + ')s', "%Y-%m-%d %H:%M:%S", \
                            attributes_list=all_attributes_list)

kusto_handler = KustoHandler(<kusto cluster name>, <kusto database>, <kusto table name>, all_attributes_list)
kusto_handler.setLevel(logging.INFO)
kusto_handler.setFormatter(formatter)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.addHandler(kusto_handler)

d = {'env':'stage', 'domain':'xyz'}
while True:
    log = input("> ")
    if log.strip().lower() != "quit":
        logger.warn(log)
        logger.info(log, extra=d)
    else:
        break

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License
[MIT](https://choosealicense.com/licenses/mit/)