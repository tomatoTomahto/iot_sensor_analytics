from KuduConnection import KuduConnection
import random

class Maintenance():

    def __init__(self, well_count, assets, kudu):
        self._well_count = well_count
        self._kudu = kudu
        self._assets = assets
        self._failed_asset = 0
        self._failed_asset_state = 0 # 0 = healthy, 1 = warning, 2 = failure
        self._start_hour = 24 # time when asset starts to spike
        self._fail_hour = 0 # time when asset shutdown
        self._end_hour = 0 # time when asset is back up

    def get_failed_asset(self):
        return self._failed_asset

    def get_failed_asset_state(self):
        return self._failed_asset_state

    def get_start_hour(self):
        return self._start_hour

    def get_fail_hour(self):
        return self._fail_hour

    def get_end_hour(self):
        return self._end_hour

    def do_maintenance(self, failure, routine_maintenance, date):
        maintenance = {}

        if self._failed_asset_state == 0:
            if routine_maintenance:
                print('routine maintenance, everything good')
                maintenance['asset_id'] = random.randint(1, len(self._assets))
                maintenance['type'] = 'ROUTINE'
                maintenance['duration'] = random.randint(1, 3)
                self._failed_asset = 0
                self._failed_asset_state = 0
                self._start_hour = 24
                self._fail_hour = 0 
                self._end_hour = 0
            elif failure:
                self._failed_asset_state = 1
                self._failed_asset = random.randint(1,len(self._assets))
                self._start_hour = random.randint(1,9)
                print('asset %d failing at %d, needs fixing' % (self._failed_asset, self._start_hour))
            else:
                self._failed_asset = 0
                self._failed_asset_state = 0
                self._start_hour = 24
                self._fail_hour = 0
                self._end_hour = 0
        elif self._failed_asset_state == 1:
            if routine_maintenance:
                print('preventative maintenance on asset %d' % self._failed_asset)
                maintenance['asset_id'] = self._failed_asset
                maintenance['type'] = 'PREVENTATIVE'
                maintenance['duration'] = random.randint(4, 6)
                self._failed_asset = 0
                self._failed_asset_state = 0
                self._start_hour = 24
                self._fail_hour = 0 
                self._end_hour = 0
            else:
                self._failed_asset_state = 0
                self._fail_hour = random.randint(1,9)
                self._end_hour = random.randint(16,24)
                self._start_hour = 24
                print('error on asset %d - shutting down asset at %d until %d' % (self._failed_asset, self._fail_hour, self._end_hour))
                print('corrective maintenance on asset %d' % self._failed_asset)
                maintenance['asset_id'] = self._failed_asset
                maintenance['type'] = 'CORRECTIVE'
                maintenance['duration'] = self._end_hour - self._fail_hour
                routine_maintenance = True

        if routine_maintenance:
            maintenance['cost'] = maintenance['duration'] * random.randint(950,1050)
            maintenance['maint_date'] = date
            self._kudu.insert('impala::sensors.maintenance', maintenance)
