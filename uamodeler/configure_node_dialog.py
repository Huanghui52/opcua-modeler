from PyQt5.QtCore import QSettings, Qt
from PyQt5.QtWidgets import QComboBox, QLabel, QLineEdit, QHBoxLayout, QDialog, QDialogButtonBox, \
    QVBoxLayout
from opcua import ua


# configure dependent node to current node
class ConfigureDialog(QDialog):
    def __init__(self, parent, title, server, plc, names):
        QDialog.__init__(self, parent)
        self.setWindowTitle(title)
        self.settings = QSettings()
        self.server = server
        self.plc = plc
        self.nodes_name = names

        self.vlayout = QVBoxLayout(self)
        self.layout = QHBoxLayout()
        self.vlayout.addLayout(self.layout)

        self.layout.addWidget(QLabel("ns:", self))

        self.nsComboBox = QComboBox(self)
        uries = server.get_namespace_array()
        for uri in uries:
            self.nsComboBox.addItem(uri)
        nsidx = int(self.settings.value("last_namespace", len(uries) - 1))
        if nsidx > len(uries) - 1:
            nsidx = len(uries) - 1
        self.nsComboBox.setCurrentIndex(nsidx)
        self.layout.addWidget(self.nsComboBox)

        self.layout.addWidget(QLabel("parent node:", self))
        self.nodeComboBox = QComboBox(self)
        for name in names:
            self.nodeComboBox.addItem(name)
        self.layout.addWidget(self.nodeComboBox)

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self)
        self.vlayout.addWidget(self.buttons)
        self.buttons.accepted.connect(self.accept)
        # self.buttons.accepted.connect(self._store_state)
        self.buttons.rejected.connect(self.reject)

    def get_args(self):
        bname, nodeid = self.get_nodeid_and_bname()
        return bname, nodeid

    def get_nodeid_and_bname(self):
        ns = self.nsComboBox.currentIndex()
        name = self.nodeComboBox.currentText()
        bname = ua.QualifiedName(name, ns)
        nodeid = self.plc.get_node_id_by_name(name)
        return bname, nodeid

    @classmethod
    def getArgs(cls, parent, title, server, plc, *args, **kwargs):
        dialog = cls(parent, title, server, plc, *args, **kwargs)
        result = dialog.exec_()
        if result == QDialog.Accepted:
            return dialog.get_args(), True
        else:
            return [], False


# set barometer data to start demo
class DemoSettingDialog(QDialog):
    def __init__(self, parent, title, server):
        QDialog.__init__(self, parent)
        self.setWindowTitle(title)
        self.settings = QSettings()
        self.server = server

        self.vlayout = QVBoxLayout(self)
        self.layout = QHBoxLayout()
        self.vlayout.addLayout(self.layout)
        self.layout.addWidget(QLabel("barometer_condition:", self))
        self.barometer_input = QLineEdit(self)
        self.barometer_input.setMinimumWidth(50)
        self.layout.addWidget(self.barometer_input)
        self.layout.addWidget(QLabel("pa", self))

        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, Qt.Horizontal, self)
        self.vlayout.addWidget(self.buttons)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)

    def get_args(self):
        data = self.barometer_input.text()
        return data

    @classmethod
    def getArgs(cls, parent, title, server, *args, **kwargs):
        dialog = cls(parent, title, server, *args, **kwargs)
        dialog.resize(300, 100)
        result = dialog.exec_()
        if result == QDialog.Accepted:
            return dialog.get_args(), True
        else:
            return [], False
