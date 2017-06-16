from KuduConnection import KuduConnection
import random

class Maintenance():

    def __init__(self, well_count, assets, kudu):
        self._well_count = well_count
        self._kudu = kudu
        self._assets = assets
        self._failed_asset = 0
        self._failed_asset_state = 0 # 0 = healthy, 1 = warning, 2 = failure
        self._start_hour = 0
        self._end_hour = 24

    def get_failed_asset(self):
        return self._failed_asset

    def get_failed_asset_state(self):
        return self._failed_asset_state

    def get_start_hour(self):
        return self._start_hour

    def get_end_hour(self):
        return self._end_hour

    def do_maintenance(self, failure, routine_maintenance, date):
        maintenance = {}

        if self._failed_asset_state == 0:
            if routine_maintenance:
                print('routine maintenance, everything good')
                maintenance['asset_id'] = random.randint(1, len(self._assets) + 1)
                maintenance['type'] = 'ROUTINE'
                maintenance['duration'] = random.randint(1, 3)
                self._failed_asset = 0
                self._failed_asset_state = 0
                self._start_hour = 0
                self._end_hour = 0
            elif failure:
                self._failed_asset_state = 1
                self._failed_asset = random.randint(1,len(self._assets)+1)
                print('asset %d failing, needs fixing' % self._failed_asset)
                self._start_hour = random.randint(1,25)
        elif self._failed_asset_state == 1:
            if routine_maintenance:
                print('preventative maintenance on asset %d' % self._failed_asset)
                maintenance['asset_id'] = self._failed_asset
                maintenance['type'] = 'PREVENTATIVE'
                maintenance['duration'] = random.randint(4, 6)
                self._failed_asset = 0
                self._failed_asset_state = 0
                self._start_hour = 0
                self._end_hour = 0
            else:
                print('error on asset %d - shutting down asset' % self._failed_asset)
                self._failed_asset_state = 2
                self._end_hour = random.randint(1,25)
                self._start_hour = 0
        elif self._failed_asset_state == 2:
            print('corrective maintenance on asset %d' % self._failed_asset)
            maintenance['asset_id'] = self._failed_asset
            maintenance['type'] = 'CORRECTIVE'
            maintenance['duration'] = random.randint(7, 9)
            routine_maintenance = True
            self._failed_asset = 0
            self._failed_asset_state = 0

        if routine_maintenance:
            maintenance['cost'] = maintenance['duration'] * 1000
            maintenance['maint_date'] = date
            self._kudu.insert('impala::sensors.maintenance', maintenance)
