import logging
import sys
import time

from PyQt5.QtCore import QObject, pyqtSignal, Qt
from PyQt5.QtWidgets import QApplication
from opcua import Client, ua

logger = logging.getLogger(__name__)


class ValveHandler(QObject):
    # signal: node val
    valve_status_fired = pyqtSignal(object, int)

    def datachange_notification(self, node, val, data):
        self.valve_status_fired.emit(node, val)


class BarometerHandler(QObject):
    barometer_data_fired = pyqtSignal(object, int)

    def datachange_notification(self, node, val, data):
        print("current barometer01 data: ", val)
        if val < 0.01:
            self.barometer_data_fired.emit(node, val)


class VacuumPumpHandler(QObject):
    pump_status_fired = pyqtSignal(object, int)

    def datachange_notification(self, node, val, data):
        self.pump_status_fired.emit(node, val)


class UaClient(object):
    def __init__(self):
        self.client = None
        self.root = None
        self.valve01 = None
        self.valve02 = None
        self.pump01 = None
        self.pump02 = None
        self.barometer01 = None
        self.valve_handler = None
        self.barometer_handler = None
        self.pump_handler = None

    def connect(self):
        self.client = Client("opc.tcp://localhost:4840/freeopcua/server/")
        self.client.connect()
        self.root = self.client.get_root_node()
        self.valve01 = self.get_valve("valve01")
        self.valve02 = self.get_valve("valve02")
        self.pump01 = self.get_pump("vacuumpump01")
        self.pump02 = self.get_pump("vacuumpump02")
        self.barometer01 = self.get_barometer("barometer01")
        self.valve_handler = ValveHandler()
        self.barometer_handler = BarometerHandler()
        self.pump_handler = VacuumPumpHandler()
        # 订阅阀门状态
        self.subscribe_data_change(self.valve01.get_child("0:ValveStatus"), self.valve_handler)
        self.subscribe_data_change(self.valve02.get_child("0:ValveStatus"), self.valve_handler)
        # 订阅气压计数值
        self.subscribe_data_change(self.barometer01.get_child(["0:BarometerData", "0:Value"]), self.barometer_handler)
        # 订阅泵状态
        self.subscribe_data_change(self.pump02.get_child("0:VacuumPumpStatus"), self.pump_handler)
        # 绑定信号和槽
        self.valve_handler.valve_status_fired.connect(self.valve_callback)
        self.barometer_handler.barometer_data_fired.connect(self.barometer_callback)
        self.pump_handler.pump_status_fired.connect(self.pump_callback)

    # 订阅阀门状态函数

    def subscribe_data_change(self, node, handler):
        data_change_sub = self.client.create_subscription(500, handler)
        handle = data_change_sub.subscribe_data_change(node)
        return handle

    def get_valve(self, name):
        valve_name = "0:" + name
        return self.root.get_child(["0:Objects", valve_name])

    def get_pump(self, name):
        pump_name = "0:" + name
        return self.root.get_child(["0:Objects", pump_name])

    def get_barometer(self, name):
        barometer_name = "0:" + name
        return self.root.get_child(["0:Objects", barometer_name])

    def valve_callback(self, node, val):
        status_enum = {
            "0": "CLOSE",
            "1": "OPEN",
            "2": "CLOSE_PROCESS",
            "3": "OPEN_PROCESS"
        }
        valve_pump_map = {
            "valve01": "vacuumpump01",
            "valve02": "vacuumpump02"
        }
        valve_name = node.get_parent().get_browse_name().Name
        print(valve_name, "status: ", status_enum.get(str(val)))
        pump_name = "0:" + valve_pump_map.get(valve_name)
        cur_pump = self.root.get_child(["0:Objects", pump_name])

        # 当阀门关闭状态
        if val == 0:
            cur_pump.call_method("0:stopVacuumPump", cur_pump.nodeid)
            print(valve_pump_map.get(valve_name), "status: CLOSE")
        elif val == 1:
            cur_pump.call_method("0:startVacuumPump", cur_pump.nodeid)
            print(valve_pump_map.get(valve_name), "status: OPEN")

    def barometer_callback(self, node, val):
        self.valve02.call_method("0:stopValve", True)

    def pump_callback(self, node, val):
        if val == 1:
            self.barometer01.call_method("0:startBarometer", 'Sheet1')

    def close_all_valve_and_pump(self):
        # 关闭所有阀门 & 真空泵
        self.valve01.call_method("0:stopValve", True)
        self.valve02.call_method("0:stopValve", True)

    def demo(self):
        # 1.关闭所有阀门 & 真空泵
        # 2.打开抽气阀门 0->3->1
        # 3.开启抽气真空泵 0->1
        # 4.获取气压计读数（1Hz）
        # 5.关闭抽气阀门
        # 6.关闭抽气真空泵
        self.close_all_valve_and_pump()
        while self.pump01.get_child("0:VacuumPumpStatus").get_value() != 0 | self.pump02.get_child(
                "0:VacuumPumpStatus").get_value() != 0:
            time.sleep(1)
        self.valve02.call_method("0:startValve", 2.0)


def demo_01():
    # 这将调用C++类的构造函数QApplication
    # 它在C++中使用sys.argv（argc和argv）初始化QT应用程序
    # 可以传递给QT一堆参数，例如样式，调试内容等等。
    app = QApplication(sys.argv)
    client = UaClient()
    client.connect()
    client.demo()
    sys.exit(app.exec_())


if __name__ == "__main__":
    demo_01()
