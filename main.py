# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 14:59:38 2017

@author: pmaldonado
"""

import zin_sklik
import sys
import traceback

try:
    app = zin_sklik.App()
    app.run()
except ValueError as err:
    print(err, file=sys.stderr)
    sys.exit(1)
except Exception as err:
    print(err, file=sys.stderr)
    traceback.print_exc(file=sys.stderr)
    sys.exit(2)