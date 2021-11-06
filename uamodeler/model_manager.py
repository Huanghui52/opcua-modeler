import logging
import os
import xml.etree.ElementTree as Et
import time
import threading
import pandas as pd
import faulthandler

from collections import OrderedDict
from PyQt5.QtCore import pyqtSignal, QObject, QSettings, QMutex
from opcua import ua, Client
from opcua import copy_node
from opcua import Node
from opcua.common.instantiate import instantiate
from opcua.common.ua_utils import data_type_to_variant_type
from opcua.common.structures import Struct, StructGenerator
from opcua.common.type_dictionary_buider import DataTypeDictionaryBuilder, get_ua_class
from opcua.common.methods import uamethod
from uawidgets.utils import trycatchslot
from uamodeler.server_manager import ServerManager
from uamodeler.xml_util import XmlCreator
from xml.dom.minidom import parse

logger = logging.getLogger(__name__)
demo = QMutex()
faulthandler.enable()


class _Struct:
    def __init__(self, name, typename):
        self.name = name
        self.typename = typename
        self.fields = []


class ModelManager(QObject):
    """
    Manage our model. loads xml, start and close, add nodes
    No dialogs at that level, only api
    """

    error = pyqtSignal(Exception)
    titleChanged = pyqtSignal(str)
    modelChanged = pyqtSignal()

    def __init__(self, modeler):
        QObject.__init__(self, modeler)
        self.modeler = modeler
        self.server_mgr = ServerManager(self.modeler.ui.actionUseOpenUa)
        self.plc_model = PlcModel(self.server_mgr)
        self.new_nodes = []  # the added nodes we will save
        self.current_path = None
        self.settings = QSettings()
        self.modified = False
        self.modeler.attrs_ui.attr_written.connect(self._attr_written)
        self.client = UaClient()
        self.xmlCreator = None

    def delete_node(self, node, interactive=True):
        logger.warning("Deleting: %s", node)
        if node:
            deleted_nodes = node.delete(delete_references=True, recursive=True)
            for dn in deleted_nodes:
                # make sure we remove ALL instances of node
                self.new_nodes[:] = (node for node in self.new_nodes if node != dn)
            if interactive:
                self.modeler.tree_ui.remove_current_item()

    def paste_node(self, node):
        parent = self.modeler.get_current_node()
        try:
            added_nodes = copy_node(parent, node)
        except Exception as ex:
            self.show_error(ex)
            raise
        self.new_nodes.extend(added_nodes)
        self.modeler.tree_ui.reload_current()
        self.modeler.show_refs()
        self.modified = True

    def close_model(self, force=False):
        if not force and self.modified:
            raise RuntimeError("Model is modified, use force to close it")
        self.modeler.actions.disable_all_actions()
        # if client is connected, disconnect it when close current model.
        if self.client.connected:
            self.client.disconnect()
        self.server_mgr.stop_server()
        self.current_path = None
        self.modified = False
        self.titleChanged.emit("")
        self.modeler.clear_all_widgets()

    def new_model(self):
        if self.modified:
            raise RuntimeError("Model is modified, cannot create new model")
        del (self.new_nodes[:])  # empty list while keeping reference

        endpoint = 'opc.tcp://0.0.0.0:4840/freeopcua/server/'
        logger.info("Starting server on %s", endpoint)
        self.server_mgr.start_server(endpoint)

        self.modeler.tree_ui.set_root_node(self.server_mgr.nodes.root)
        self.modeler.idx_ui.set_node(self.server_mgr.get_node(ua.ObjectIds.Server_NamespaceArray))
        self.modeler.nodesets_ui.set_server_mgr(self.server_mgr)
        self.modified = False
        self.modeler.actions.enable_model_actions()
        self.current_path = None
        self.titleChanged.emit("No Name")
        return True

    def import_xml(self, path):
        new_nodes = self.server_mgr.import_xml(path)
        self.new_nodes.extend([self.server_mgr.get_node(node) for node in new_nodes])
        self.modified = True
        # we maybe should only reload the imported nodes
        self.modeler.tree_ui.reload()
        self.modeler.idx_ui.reload()
        return path

    def open_xml(self, path):
        self.new_model()
        try:
            self._open_xml(path)
        except:
            self.close_model(force=True)
            raise

    def _open_xml(self, path):
        path = self.import_xml(path)
        self.server_mgr.load_enums()
        self.server_mgr.load_type_definitions()
        self._show_structs()
        self.modified = False
        self.current_path = path
        self.titleChanged.emit(self.current_path)
        # self.plc_model.create_plc_model()
        self.link_xml_method()
        # 通过添加对象时触发event订阅，链接demo节点和plc model的函数，通知client端刷新节点
        self.plc_model.create_plc_event()
        self.server_mgr.link_method(self.server_mgr.get_objects_node().get_child("0:AddObjectNotification"),
                                    self.plc_model.standard_to_extreme)
        # 创建一个和模型对应的mapping.xml映射文件 .uamodel后缀文件 是\\
        model_xml_name = path[path.rfind('/') + 1: len(path)]
        model_name = model_xml_name[0: model_xml_name.find('.')]
        relation_xml_name = model_name + "_mapping.xml"
        self.xmlCreator = XmlCreator(relation_xml_name)

    def link_xml_method(self):
        xml_nodes = self.server_mgr.get_objects_node().get_children()
        for node in xml_nodes:
            if node.get_type_definition() == ua.NodeId.from_string("ns=1;i=2007"):
                self.plc_model.link_valve_method(node)
            if node.get_type_definition() == ua.NodeId.from_string("ns=1;i=2015"):
                self.plc_model.link_barometer_method(node)
            if node.get_type_definition() == ua.NodeId.from_string("ns=1;i=2020"):
                self.plc_model.link_vacuum_pump_method(node)

    def _show_structs(self):
        base_struct = self.server_mgr.get_node(ua.ObjectIds.Structure)
        opc_binary = self.server_mgr.get_node(ua.ObjectIds.OPCBinarySchema_TypeSystem)
        opc_schema = self.server_mgr.get_node(ua.ObjectIds.OpcUa_BinarySchema)
        for node in opc_binary.get_children():
            if node == opc_schema:
                continue  # This is standard namespace structures
            try:
                ns = node.get_child("0:NamespaceUri").get_value()
                ar = self.server_mgr.get_namespace_array()
                idx = ar.index(ns)
            except ua.UaError:
                idx = 1
            xml = node.get_value()
            if not xml:
                return

            xml = xml.decode("utf-8")
            generator = StructGenerator()
            generator.make_model_from_string(xml)
            for el in generator.model:
                # we only care about structs, ignoring enums
                if isinstance(el, Struct):
                    self._add_design_node(base_struct, idx, el)

    def _add_design_node(self, base_struct, idx, el):
        try:
            struct_node = base_struct.get_child(f"{idx}:{el.name}")
        except ua.UaError:
            logger.warning("Could not find struct %s under %s", el.name, base_struct)
            return
        for field in el.fields:
            if hasattr(ua.ObjectIds, field.uatype):
                dtype = self.server_mgr.get_node(getattr(ua.ObjectIds, field.uatype))
            else:
                dtype = self._get_datatype_from_string(idx, field.uatype)
                if not dtype:
                    logger.warning("Could not find datatype of name %s %s", field.uatype, type(field.uatype))
                    return
            vtype = data_type_to_variant_type(dtype)
            val = ua.get_default_value(vtype)
            node = struct_node.add_variable(idx, field.name, val, varianttype=vtype, datatype=dtype.nodeid)
            if field.array:
                node.set_value_rank(ua.ValueRank.OneDimension)
                node.set_array_dimensions([1])

    def _get_datatype_from_string(self, idx, name):
        # FIXME: this is very heavy and missing recusion, what is the correct way to do that?
        for node in self.server_mgr.get_node(ua.ObjectIds.BaseDataType).get_children():
            try:
                dtype = node.get_child(f'{idx}:{name}')
            except ua.UaError:
                continue
            return dtype
        return None

    def open(self, path):
        if path.endswith(".xml"):
            self.open_xml(path)
        else:
            self.open_ua_model(path)

    def open_ua_model(self, path):
        self.new_model()
        try:
            self._open_ua_model(path)
        except:
            self.close_model(force=True)
            raise

    def _open_ua_model(self, path):
        tree = Et.parse(path)
        root = tree.getroot()
        for ref_el in root.findall("Reference"):
            refpath = ref_el.attrib['path']
            self.modeler.nodesets_ui.import_nodeset(refpath)
        mod_el = root.find("Model")
        dirname = os.path.dirname(path)
        xmlpath = os.path.join(dirname, mod_el.attrib['path'])
        xmlpath = xmlpath.replace('\\', '/')
        self._open_xml(xmlpath)
        if "current_node" in mod_el.attrib:
            current_node_str = mod_el.attrib['current_node']
            nodeid = ua.NodeId.from_string(current_node_str)
            current_node = self.server_mgr.get_node(nodeid)
            self.modeler.tree_ui.expand_to_node(current_node)

    def _get_path(self, path):
        if path is None:
            path = self.current_path
        if path is None:
            raise ValueError("No path is defined")
        self.current_path = os.path.splitext(path)[0]
        self.titleChanged.emit(self.current_path)
        return self.current_path

    def save_xml(self, path=None):
        self._save_structs()
        path = self._get_path(path)
        path += ".xml"
        logger.info("Saving nodes to %s", path)
        logger.info("Exporting  %s nodes: %s", len(self.new_nodes), self.new_nodes)
        logger.info("and namespaces: %s ", self.server_mgr.get_namespace_array()[1:])
        uris = self.server_mgr.get_namespace_array()[1:]
        self.new_nodes = list(OrderedDict.fromkeys(self.new_nodes))  # remove any potential duplicate
        self.server_mgr.export_xml(self.new_nodes, uris, path)
        self.modified = False
        logger.info("%s saved", path)
        self._show_structs()  # _save_structs has delete our design nodes for structure, we need to recreate them

    def save_ua_model(self, path=None):
        path = self._get_path(path)
        model_path = path + ".uamodel"
        logger.info("Saving model to %s", model_path)
        etree = Et.ElementTree(Et.Element('UAModel'))
        node_el = Et.SubElement(etree.getroot(), "Model")
        node_el.attrib["path"] = os.path.basename(path) + ".xml"
        c_node = self.modeler.tree_ui.get_current_node()
        if c_node:
            node_el.attrib["current_node"] = c_node.nodeid.to_string()
        for refpath in self.modeler.nodesets_ui.nodesets:
            node_el = Et.SubElement(etree.getroot(), "Reference")
            node_el.attrib["path"] = refpath
        etree.write(model_path, encoding='utf-8', xml_declaration=True)
        return model_path

    def _after_add(self, new_nodes):
        if isinstance(new_nodes, (list, tuple)):
            for node in new_nodes:
                if node not in self.new_nodes:
                    self.new_nodes.append(node)
        else:
            if new_nodes not in self.new_nodes:
                self.new_nodes.append(new_nodes)
        self.modeler.tree_ui.reload_current()
        self.modeler.show_refs()
        self.modified = True

    def add_method(self, *args):
        logger.info("Creating method type with args: %s", args)
        parent = self.modeler.tree_ui.get_current_node()
        new_nodes = []
        new_node = parent.add_method(*args)
        new_nodes.append(new_node)
        new_nodes.extend(new_node.get_children())
        self._after_add(new_nodes)
        return new_nodes

    def add_object_type(self, *args):
        logger.info("Creating object type with args: %s", args)
        parent = self.modeler.tree_ui.get_current_node()
        new_node = parent.add_object_type(*args)
        self._after_add(new_node)
        return new_node

    def add_folder(self, *args):
        parent = self.modeler.tree_ui.get_current_node()
        logger.info("Creating folder with args: %s", args)
        new_node = parent.add_folder(*args)
        self._after_add(new_node)
        return new_node

    def add_object(self, *args):
        parent = self.modeler.tree_ui.get_current_node()
        logger.info("Creating object with args: %s", args)
        nodeid, bname, otype = args
        new_nodes = instantiate(parent, otype, bname=bname, nodeid=nodeid, dname=ua.LocalizedText(bname.Name))
        self._after_add(new_nodes)
        # link method
        self.add_object_link_method(new_nodes)
        # update root node
        self.modeler.tree_ui.set_root_node(self.server_mgr.nodes.root)
        self.modeler.tree_ui.expand_to_node(self.server_mgr.nodes.objects)
        # trigger event
        self.add_object_event_trigger()
        return new_nodes

    def add_object_link_method(self, nodes):
        for node in nodes:
            if node.get_type_definition() == ua.NodeId.from_string("ns=1;i=2007"):
                self.plc_model.link_valve_method(node)
            elif node.get_type_definition() == ua.NodeId.from_string("ns=1;i=2015"):
                self.plc_model.link_barometer_method(node)
            elif node.get_type_definition() == ua.NodeId.from_string("ns=1;i=2020"):
                self.plc_model.link_vacuum_pump_method(node)

    def add_object_event_trigger(self):
        root = self.server_mgr.get_root_node()
        etype = root.get_child(["0:Types", "0:EventTypes", "0:BaseEventType", "0:AddObjectEvent"])
        event_gen = self.server_mgr.get_event_generator(etype, self.server_mgr.get_server_node())
        event_gen.event.Message = ua.LocalizedText("AddObjectEvent")
        event_gen.trigger()

    def add_data_type(self, *args):
        parent = self.modeler.tree_ui.get_current_node()
        logger.info("Creating data type with args: %s", args)
        new_node = parent.add_data_type(*args)
        self._after_add(new_node)
        return new_node

    def add_variable(self, *args):
        parent = self.modeler.tree_ui.get_current_node()
        logger.info("Creating variable with args: %s", args)
        new_node = parent.add_variable(*args)
        self._after_add(new_node)
        return new_node

    def add_property(self, *args):
        parent = self.modeler.tree_ui.get_current_node()
        logger.info("Creating property with args: %s", args)
        new_node = parent.add_property(*args)
        self._after_add(new_node)
        return new_node

    def add_variable_type(self, *args):
        parent = self.modeler.tree_ui.get_current_node()
        logger.info("Creating variable type with args: %s", args)
        nodeid, bname, datatype = args
        new_node = parent.add_variable_type(nodeid, bname, datatype.nodeid)
        self._after_add(new_node)
        return new_node

    @trycatchslot
    def _attr_written(self, attr, dv):
        self.modified = True
        if attr == ua.AttributeIds.BrowseName:
            self.modeler.tree_ui.update_browse_name_current_item(dv.Value.Value)
        elif attr == ua.AttributeIds.DisplayName:
            self.modeler.tree_ui.update_display_name_current_item(dv.Value.Value)

    def _create_type_dict_node(self, idx, urn, name):
        node_id = None
        # first delete current dict node and its children
        try:
            opc_binary = self.server_mgr.get_node(ua.ObjectIds.OPCBinarySchema_TypeSystem)
            dnode = opc_binary.get_child(f"{idx}:{name}")
            node_id = dnode.nodeid
        except ua.UaError:
            logger.warning("Dictionary node does not exist, creating it: %s", name)
        builder = DataTypeDictionaryBuilder(self.server_mgr, idx, urn, name, dict_node_id=node_id)
        if builder.dict_id not in self.new_nodes:
            self.new_nodes.append(self.server_mgr.get_node(builder.dict_id))
        return builder

    def _save_structs(self):
        """
        Save struct and delete our design nodes. They will need to be recreated
        """
        struct_node = self.server_mgr.get_node(ua.ObjectIds.Structure)
        dict_name = "TypeDictionary"
        idx = 1
        urn = self.server_mgr.get_namespace_array()[0]
        to_delete = []
        have_structs = False
        to_add = []
        for node in self.new_nodes:
            # FIXME: we do not support inheritance
            parent = node.get_parent()
            if parent == struct_node:
                if not have_structs:
                    dict_builder = self._create_type_dict_node(idx, urn, dict_name)
                    dict_node = self.server_mgr.get_node(dict_builder.dict_id)
                have_structs = True
                bname = node.get_browse_name()
                try:
                    dict_node.get_child(f"{idx}:{bname.Name}")
                    struct = dict_builder.create_data_type(bname.Name, node.nodeid, init=False)
                except ua.UaError:
                    logger.warning("DataType %s has not been initialized, doing it", bname)
                    struct = dict_builder.create_data_type(bname.Name, node.nodeid, init=True)

                childs = node.get_children()
                for child in childs:
                    bname = child.get_browse_name()
                    try:
                        dtype = child.get_data_type()
                    except ua.UaError:
                        logger.warning("could not get data type for node %s, %s, skipping", child,
                                       child.get_browse_name())
                        continue
                    array = False
                    if isinstance(child.get_value(),
                                  list) or child.get_array_dimensions() or child.get_value_rank() != ua.ValueRank.Scalar:
                        array = True

                    dtype_name = Node(node.server, dtype).get_browse_name()
                    struct.add_field(bname.Name, dtype_name.Name, is_array=array)
                    to_delete.append(child)

                to_add.extend([self.server_mgr.get_node(nodeid) for nodeid in struct.node_ids])

        if have_structs:
            dict_builder.set_dict_byte_string()
            self.new_nodes.extend(to_add)

        for node in to_delete:
            self.delete_node(node, False)

    def configure_node(self, *args):
        bname, nodeid = args
        current_node = self.modeler.tree_ui.get_current_node()
        print("parent name:", bname.Name, "child name:", current_node.get_browse_name().Name)
        self.xmlCreator.create_relation_item(nodeid, bname.Name)
        self.xmlCreator.add_child_item(nodeid, bname.Name, current_node.nodeid, current_node.get_browse_name().Name)
        self.xmlCreator.write_xml()
        logger.info(self.xmlCreator.xml_name + " has updated.")

    def exhaust_demo(self, data):
        parse_result = self.pre_execute_demo()
        if not parse_result:
            return
        self.client.demoNum = 1
        self.client.exhaust_demo(data)

    def intake_demo(self, data):
        parse_result = self.pre_execute_demo()
        if not parse_result:
            return
        self.client.demoNum = 2
        self.client.intake_demo(data)

    def pre_execute_demo(self):
        self.plc_model.stop_barometer_flag = False
        if self.client.connected:
            self.client.disconnect()
        self.client = UaClient()
        self.client.connect()
        parse_result = self.client.parse_xml(self.xmlCreator.xml_name)
        return parse_result


def change_valve_status(status):
    status_value = status.get_value()
    if status_value == ua.StatusEnum.OPEN_PROCESS:
        time.sleep(3)
        status.set_value(1)
    elif status_value == ua.StatusEnum.CLOSE_PROCESS:
        time.sleep(3)
        status.set_value(0)


class ChangeValveStatus(threading.Thread):
    def __init__(self, status):
        threading.Thread.__init__(self)
        self.status = status

    def run(self):
        time.sleep(3)
        status_value = self.status.get_value()
        if status_value == ua.StatusEnum.OPEN_PROCESS:
            self.status.set_value(1)
        elif status_value == ua.StatusEnum.CLOSE_PROCESS:
            self.status.set_value(0)


class BarometerDataThread(threading.Thread):
    def __init__(self, barometer_value, data, stop_flag):
        threading.Thread.__init__(self)
        self.barometer_value = barometer_value
        self.data = data
        self.stop_flag = stop_flag

    def run(self):
        for v in self.data['value']:
            if not self.stop_flag:
                self.barometer_value.set_value(v, ua.VariantType.Float)
                time.sleep(1)
            else:
                return


class PlcModel(object):
    def __init__(self, server):
        self.server_mgr = server
        self.valve01 = None
        self.valve02 = None
        self.barometer01 = None
        self.pump01 = None
        self.pump02 = None
        self.standard_to_extreme_thread = threading.Thread(target=self._standard_to_extreme)
        self.settings = QSettings()
        self.start_barometer_thread = None
        self.stop_barometer_flag = False

    def create_plc_event(self):
        self.server_mgr.create_custom_event_type(0, 'AddObjectEvent')

    def create_plc_model(self):
        # 阀门初始化
        my_valve_type = (self.server_mgr.nodes.base_object_type.get_child(["0:Valve"])).nodeid
        self.valve01 = self.server_mgr.nodes.objects.add_object(0, "valve01", my_valve_type)
        self.valve02 = self.server_mgr.nodes.objects.add_object(0, "valve02", my_valve_type)
        # 修改阀门类型
        valve01_type = self.valve01.get_child(["0:ValveType"])
        # ua.ValveTypeEnum.VENT
        valve01_type.set_value(1)
        # method调用
        self.link_valve_method(self.valve01)
        self.link_valve_method(self.valve02)

        # 气压计初始化
        my_barometer_type = (self.server_mgr.nodes.base_object_type.get_child(["0:Barometer"])).nodeid
        self.barometer01 = self.server_mgr.nodes.objects.add_object(0, "barometer01", my_barometer_type)
        barometer01_status = self.barometer01.get_child("0:BarometerStatus")
        barometer01_status.set_value(0)
        self.link_barometer_method(self.barometer01)

        # 真空泵初始化
        my_pump_type = (self.server_mgr.nodes.base_object_type.get_child("0:VacuumPump")).nodeid
        self.pump01 = self.server_mgr.nodes.objects.add_object(0, "vacuumpump01", my_pump_type)
        self.pump02 = self.server_mgr.nodes.objects.add_object(0, "vacuumpump02", my_pump_type)
        self.link_vacuum_pump_method(self.pump01)
        self.link_vacuum_pump_method(self.pump02)

    @uamethod
    def startValve(self, parent, gasflow):
        status = self.server_mgr.get_node(parent).get_child("0:ValveStatus")
        status_value = status.get_value()
        if status_value == ua.StatusEnum.CLOSE:
            status.set_value(ua.StatusEnum.OPEN_PROCESS)
            # OPEN_PROCESS -> wait -> OPEN
            start_valve_thread = ChangeValveStatus(status)
            start_valve_thread.start()
            # start_valve_thread = threading.Thread(target=change_valve_status, args=(status,))
            # start_valve_thread.setDaemon(True)
            # start_valve_thread.start()
            config = self.server_mgr.get_node(parent).get_child(["0:ValveConfig", "0:GasFlow"])
            config.set_value(gasflow, ua.VariantType.Float)
            return "The valve opens and set the gas flow rate to " + str(gasflow) + " L/m"
        return "The valve is opened."

    @uamethod
    def stopValve(self, parent, stopped):
        status = self.server_mgr.get_node(parent).get_child("0:ValveStatus")
        status_value = status.get_value()
        config = self.server_mgr.get_node(parent).get_child(["0:ValveConfig", "0:GasFlow"])
        config.set_value(0, ua.VariantType.Float)
        if status_value == ua.StatusEnum.OPEN:
            status.set_value(ua.StatusEnum.CLOSE_PROCESS)
            stop_valve_thread = ChangeValveStatus(status)
            stop_valve_thread.start()
            # stop_valve_thread = threading.Thread(target=change_valve_status, args=(status,))
            # stop_valve_thread.setDaemon(True)
            # stop_valve_thread.start()
            return "The valve closes and set the gas flow rate to 0 L/m"
        return "The valve is closed."

    @uamethod
    def startBarometer(self, parent, sheet):
        # close -> open
        status = self.server_mgr.get_node(parent).get_child("0:BarometerStatus")
        status_value = status.get_value()
        if status_value == ua.StatusEnum.CLOSE:
            status.set_value(ua.StatusEnum.OPEN)
            # close->open的时候才起读取数据线程
            # start display data
            value = self.server_mgr.get_node(parent).get_child(["0:BarometerData", "0:Value"])
            data = pd.read_excel('barometer_data/barometer.xlsx', sheet_name=sheet)
            start_barometer_thread = BarometerDataThread(value, data, self.stop_barometer_flag)
            start_barometer_thread.start()
            # self.start_barometer_thread = threading.Thread(target=self.getData, args=(data, df, ))
            # self.start_barometer_thread.setDaemon(True)
            # self.start_barometer_thread.start()

    # def getData(self, data, df):
        # data = self.server_mgr.get_node(parent).get_child(["0:BarometerData", "0:Value"])
         # df = pd.read_excel('barometer_data/barometer.xlsx', sheet_name=sheet)
        # for v in df['value']:
        #     if not self.stop_barometer_flag:
        #         data.set_value(v, ua.VariantType.Float)
        #         time.sleep(1)
        #     else:
        #         return

    @uamethod
    def stopBarometer(self, parent, kpa):
        status = self.server_mgr.get_node(parent).get_child("0:BarometerStatus")
        status_value = status.get_value()
        if status_value == ua.StatusEnum.OPEN:
            status.set_value(0)
            # 杀死读取数据进程
            self.stop_barometer_flag = True
            # raise_exception(self.start_barometer_thread.ident)

    @uamethod
    def startVacuumPump(self, parent, speed):
        status = self.server_mgr.get_node(parent).get_child("0:VacuumPumpStatus")
        status_value = status.get_value()
        if status_value == ua.StatusEnum.CLOSE:
            status.set_value(ua.StatusEnum.OPEN)
            config = self.server_mgr.get_node(parent).get_child(["0:VacuumPumpConfig", "0:FREQ"])
            config.set_value(speed, ua.VariantType.Float)
            return "The vacuum pump opens and set the gas flow rate to " + str(speed) + " L/m"
        return "The vacuum pump is opened."

    @uamethod
    def stopVacuumPump(self, parent, speed):
        status = self.server_mgr.get_node(parent).get_child("0:VacuumPumpStatus")
        status_value = status.get_value()
        if status_value == ua.StatusEnum.OPEN:
            status.set_value(0)
            return "The vacuum pump opens and set the gas flow rate to 0L/m"
        return "The vacuum pump is closed."

    def link_valve_method(self, node):
        node_start = node.get_child("0:startValve")
        self.server_mgr.link_method(node_start, self.startValve)
        node_stop = node.get_child("0:stopValve")
        self.server_mgr.link_method(node_stop, self.stopValve)

    def link_barometer_method(self, node):
        node_start = node.get_child("0:startBarometer")
        self.server_mgr.link_method(node_start, self.startBarometer)
        node_stop = node.get_child("0:stopBarometer")
        self.server_mgr.link_method(node_stop, self.stopBarometer)

    def link_vacuum_pump_method(self, node):
        node_start = node.get_child("0:startVacuumPump")
        self.server_mgr.link_method(node_start, self.startVacuumPump)
        node_stop = node.get_child("0:stopVacuumPump")
        self.server_mgr.link_method(node_stop, self.stopVacuumPump)

    def get_nodes_by_type(self, node_type):
        children = self.server_mgr.get_objects_node().get_children()
        nodes = []
        for node in children:
            if node.get_type_definition() == ua.NodeId.from_string(node_type):
                nodes.append(node)
        return nodes

    def get_node_id_by_name(self, node_name):
        children = self.server_mgr.get_objects_node().get_children()
        for node in children:
            if node.get_browse_name().Name == node_name:
                return node.nodeid

    @uamethod
    def standard_to_extreme(self, parent):
        # This method returns True just before the run() method starts until just after the run() method terminates.
        if self.standard_to_extreme_thread.isAlive():
            return False
        self.standard_to_extreme_thread = threading.Thread(target=self._standard_to_extreme)
        self.standard_to_extreme_thread.start()
        return True

    def _standard_to_extreme(self):
        # 加入线程锁
        demo.lock()
        # 1.关闭所有阀门 & 真空泵
        self.stopValve(self.valve01.nodeid, ua.Variant(True, ua.VariantType.Boolean))
        self.stopValve(self.valve02.nodeid, ua.Variant(True, ua.VariantType.Boolean))
        valve01_status = self.valve01.get_child("0:ValveStatus")
        valve02_status = self.valve02.get_child("0:ValveStatus")
        while valve01_status.get_value() != 0 | valve02_status.get_value() != 0:
            time.sleep(1)
        self.stopVacuumPump(self.pump01.nodeid, ua.Variant(True, ua.VariantType.Boolean))
        self.stopVacuumPump(self.pump02.nodeid, ua.Variant(True, ua.VariantType.Boolean))
        logger.info("close all valves and vacuum pumps.")

        # 2.打开抽气阀门 0->3->1
        self.startValve(self.valve02.nodeid, ua.Variant(0, ua.VariantType.Float))
        while self.valve02.get_child("0:ValveStatus").get_value() != 1:
            time.sleep(1)
        logger.info("open vent valve.")

        # 3.开启抽气真空泵 0->1
        self.startVacuumPump(self.pump02.nodeid, ua.Variant(2.0, ua.VariantType.Float))
        valve02_gas_flow = self.valve02.get_child(["0:ValveConfig", "0:GasFlow"])
        valve02_gas_flow.set_value(2.0, ua.VariantType.Float)
        logger.info("open vent vacuum pump.")

        # 4.获取气压计读数（1Hz）
        self.startBarometer(self.barometer01.nodeid, ua.Variant('Sheet1', ua.VariantType.String))
        barometer01_data = self.barometer01.get_child(["0:BarometerData", "0:Value"])
        while barometer01_data.get_value() >= 0.01:
            logger.info("Current barometer data is %r pa", barometer01_data.get_value())
            time.sleep(1)

        # 5.关闭抽气阀门
        self.stopValve(self.valve02.nodeid, ua.Variant(True, ua.VariantType.Boolean))
        while valve02_status.get_value() != 0:
            time.sleep(1)
        logger.info("close vent valve.")

        # 6.关闭抽气真空泵
        self.stopVacuumPump(self.pump02.nodeid, ua.Variant(True, ua.VariantType.Boolean))
        logger.info("close vent vacuum pump.")
        demo.unlock()


def get_node_id_attr(nodeid):
    return "ns=" + str(nodeid.NamespaceIndex) + ";i=" + str(nodeid.Identifier)


class UaClient(object):
    def __init__(self):
        self.client = None
        self.root = None
        # 当前仅支持一个气压计
        self.barometer = None
        self.valve_handler = None
        self.barometer_handler = None
        self.pump_handler = None
        self.barometer_condition = None
        self.all_nodes = []
        self.all_nodes_id = []
        self.relation_root = []
        self.subscriptions = []
        self.is_under_barometer = False
        self.is_beyond_barometer = False
        self.connected = False
        self.demoNum = 0

    def connect(self):
        self.client = Client("opc.tcp://localhost:4840/freeopcua/server/")
        self.client.connect()
        self.root = self.client.get_root_node()
        self.valve_handler = ValveHandler()
        self.barometer_handler = BarometerHandler()
        self.pump_handler = VacuumPumpHandler()
        self.barometer_condition = 0.01
        # 订阅阀门状态
        self.subscribe_valve_status()
        # 订阅气压计数值
        self.subscribe_barometer_data()
        # 订阅泵状态
        self.subscribe_pump_status()
        # 绑定信号和槽
        self.valve_handler.valve_status_fired.connect(self.valve_callback)
        self.barometer_handler.barometer_data_fired.connect(self.barometer_callback)
        self.pump_handler.pump_status_fired.connect(self.pump_callback)
        self.connected = True

    def disconnect(self):
        self.delete_all_subscription()
        if self.client is not None:
            self.client.disconnect()
            self.connected = False

    # 订阅阀门状态函数
    def subscribe_valve_status(self):
        valve_nodes = self.get_all_valves()
        for valve_node in valve_nodes:
            self.subscribe_data_change(valve_node.get_child("0:ValveStatus"), self.valve_handler)

    def subscribe_pump_status(self):
        pump_nodes = self.get_all_pumps()
        for pump_node in pump_nodes:
            self.subscribe_data_change(pump_node.get_child("0:VacuumPumpStatus"), self.pump_handler)

    def subscribe_barometer_data(self):
        barometer_nodes = self.get_all_barometers()
        for barometer_node in barometer_nodes:
            self.subscribe_data_change(barometer_node.get_child(["0:BarometerData", "0:Value"]), self.barometer_handler)

    def subscribe_data_change(self, node, handler):
        data_change_sub = self.client.create_subscription(500, handler)
        handle = data_change_sub.subscribe_data_change(node)
        self.subscriptions.append(data_change_sub)
        return handle

    def delete_all_subscription(self):
        for sub in self.subscriptions:
            sub.delete()

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
        # node是阀门状态
        status_enum = {
            "0": "CLOSE",
            "1": "OPEN",
            "2": "CLOSE_PROCESS",
            "3": "OPEN_PROCESS"
        }
        logger.info("%s status: %s", node.get_parent().get_browse_name().Name, status_enum.get(str(val)))
        # 解析xml获取节点映射关系
        valve_node_id = node.get_parent().nodeid
        cur_index = self.all_nodes_id.index(get_node_id_attr(valve_node_id)) if get_node_id_attr(
            valve_node_id) in self.all_nodes_id else -1
        if cur_index == -1:
            return
        cur_mapping = self.all_nodes[cur_index]
        cur_child_nodes = cur_mapping.child_nodes
        for child in cur_child_nodes:
            child_node = self.client.get_node(ua.NodeId.from_string(child.parent_node_id))
            # child: valve
            if child_node.get_type_definition() == ua.NodeId.from_string("ns=1;i=2007"):
                # 当前阀门关闭
                if val == 1:
                    child_node.call_method("0:startValve", child_node.nodeid)
                elif val == 0:
                    child_node.call_method("0:stopValve", child_node.nodeid)
            # child: pump
            if child_node.get_type_definition() == ua.NodeId.from_string("ns=1;i=2020"):
                if val == 1:
                    child_node.call_method("0:startVacuumPump", child_node.nodeid)
                elif val == 0:
                    child_node.call_method("0:stopVacuumPump", child_node.nodeid)

    def barometer_callback(self, node, val):
        # 抽气过程 当气压小于设置值时 关闭抽气阀门和气压计
        if self.demoNum == 1:
            if val < int(self.barometer_condition) and self.is_under_barometer is False:
                self.is_under_barometer = True
                # 关闭relation_root中的抽气阀门
                for valve in self.relation_root:
                    valve_node = self.client.get_node(ua.NodeId.from_string(valve.parent_node_id))
                    if valve_node.get_child("0:ValveType").get_value() == ua.ValveTypeEnum.VENT:
                        valve_node.call_method("0:stopValve", True)
                self.barometer.call_method("0:stopBarometer", True)

        # 进气过程 当气压大于设置值时 关闭进气阀门和气压计
        if self.demoNum == 2:
            if val > int(self.barometer_condition) and self.is_beyond_barometer is False:
                self.is_beyond_barometer = True
                for valve in self.relation_root:
                    valve_node = self.client.get_node(ua.NodeId.from_string(valve.parent_node_id))
                    if valve_node.get_child("0:ValveType").get_value() == ua.ValveTypeEnum.INLET:
                        valve_node.call_method("0:stopValve", True)
                self.barometer.call_method("0:stopBarometer", True)

    def pump_callback(self, node, val):
        if val == 1:
            logger.info("%s status: OPEN", node.get_parent().get_browse_name().Name)
            self.barometer.call_method("0:startBarometer", 'Sheet' + str(self.demoNum))
        else:
            logger.info("%s status: CLOSE", node.get_parent().get_browse_name().Name)

    def close_all_valves(self):
        # 关闭所有阀门 & 真空泵
        for valve in self.get_all_valves():
            valve.call_method("0:stopValve", True)

    def get_all_valves(self):
        return self.get_nodes_by_type("ns=1;i=2007")

    def get_all_barometers(self):
        return self.get_nodes_by_type("ns=1;i=2015")

    def get_all_pumps(self):
        return self.get_nodes_by_type("ns=1;i=2020")

    def get_all_vent_valves(self):
        vent_valves = []
        valves = self.get_all_valves()
        for valve in valves:
            if valve.get_child(['0:ValveType']) == ua.ValveTypeEnum.VENT:
                vent_valves.append(valve)
        return vent_valves

    def get_nodes_by_type(self, node_type):
        children = self.client.get_objects_node().get_children()
        nodes = []
        for node in children:
            if node.get_type_definition() == ua.NodeId.from_string(node_type):
                nodes.append(node)
        return nodes

    def check_all_pumps_close(self):
        all_pumps = self.get_all_pumps()
        for pump in all_pumps:
            if pump.get_child("0:VacuumPumpStatus").get_value() != 0:
                return False
        return True

    def check_all_valves_close(self):
        all_valves = self.get_all_valves()
        for valve in all_valves:
            if valve.get_child("0:ValveStatus").get_value() != 0:
                return False
        return True

    def exhaust_demo(self, barometer_condition):
        # 1.关闭所有阀门 & 真空泵
        # 2.打开抽气阀门 0->3->1
        # 3.开启抽气真空泵 0->1
        # 4.获取气压计读数（1Hz）
        # 5.关闭抽气阀门
        # 6.关闭抽气真空泵
        barometer_value = self.set_barometer(barometer_condition)
        if barometer_value < int(self.barometer_condition):
            logger.info("Current barometer data is under %s pa, don't need to execute this demo.",
                        self.barometer_condition)
            return
        self.check_demo_prepared()

        for valve in self.relation_root:
            valve_node = self.client.get_node(ua.NodeId.from_string(valve.parent_node_id))
            if valve_node.get_child("0:ValveType").get_value() == ua.ValveTypeEnum.VENT:
                valve_node.call_method("0:startValve", 2.0)

    def intake_demo(self, barometer_condition):
        # 1.关闭所有阀门 & 真空泵
        # 2.打开进气阀门
        # 3.打开进气真空泵
        # 4.获取气压计读数
        # 5.关闭进气阀门
        # 6.关闭进气真空泵
        barometer_value = self.set_barometer(barometer_condition)
        if barometer_value > int(self.barometer_condition):
            logger.info("Current barometer data is over %s pa, don't need to execute this demo.",
                        self.barometer_condition)
            return
        self.check_demo_prepared()

        for valve in self.relation_root:
            valve_node = self.client.get_node(ua.NodeId.from_string(valve.parent_node_id))
            if valve_node.get_child("0:ValveType").get_value() == ua.ValveTypeEnum.INLET:
                valve_node.call_method("0:startValve", 2.0)

    def set_barometer(self, barometer_condition):
        self.barometer_condition = barometer_condition
        self.barometer = self.get_all_barometers().__getitem__(0)
        return self.barometer.get_child(["0:BarometerData", "0:Value"]).get_value()

    def check_demo_prepared(self):
        if self.check_all_valves_close() & self.check_all_pumps_close():
            # 数据订阅打印初始状态
            time.sleep(1)
        else:
            self.close_all_valves()
            while not (self.check_all_valves_close() & self.check_all_pumps_close()):
                time.sleep(1)

    def parse_xml(self, xml_name):
        mapping_xml_path = "model_set/" + xml_name
        if os.path.isfile(mapping_xml_path) is False:
            logger.info("No such file: '%s', please configure nodes.", mapping_xml_path)
            return False
        dom_tree = parse(mapping_xml_path)
        collection = dom_tree.documentElement
        relations = collection.getElementsByTagName("relation")

        for relation in relations:
            current_parent_id = relation.getAttribute('NodeId')
            current_parent = self.get_correct_node(current_parent_id)

            children = relation.getElementsByTagName("child")
            for child in children:
                current_child_id = child.getAttribute("NodeId")
                current_child = self.get_correct_node(current_child_id)
                # 标记有父节点
                current_child.has_parent = True
                current_parent.append_child_nodes(current_child)

        for node in self.all_nodes:
            if not node.has_parent:
                self.relation_root.append(node)

        return True

    def get_correct_node(self, cur_id):
        current_index = self.all_nodes_id.index(cur_id) if cur_id in self.all_nodes_id else -1
        if current_index == -1:
            current_mapping = RelationMapping(cur_id, [])
            self.all_nodes_id.append(cur_id)
            self.all_nodes.append(current_mapping)
            return current_mapping
        else:
            return self.all_nodes[current_index]


class ValveHandler(QObject):
    # signal: node val
    valve_status_fired = pyqtSignal(object, int)

    def datachange_notification(self, node, val, data):
        self.valve_status_fired.emit(node, val)


class BarometerHandler(QObject):
    barometer_data_fired = pyqtSignal(object, int)

    def datachange_notification(self, node, val, data):
        logger.info("Current barometer data: %s pa.", val)
        self.barometer_data_fired.emit(node, val)


class VacuumPumpHandler(QObject):
    pump_status_fired = pyqtSignal(object, int)

    def datachange_notification(self, node, val, data):
        self.pump_status_fired.emit(node, val)


class RelationMapping(object):
    def __init__(self, parent_id, mapping):
        self.parent_node_id = parent_id
        self.child_nodes = mapping
        self.has_parent = False

    def append_child_nodes(self, child):
        self.child_nodes.append(child)
