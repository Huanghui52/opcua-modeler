import os
from xml.dom.minidom import Document, parse
import xml.sax


class XmlCreator:
    def __init__(self, xml_name):
        self.xml_name = xml_name
        if os.path.isfile('model_set/' + self.xml_name):
            self.doc = parse('model_set/' + self.xml_name)
        else:
            self.doc = Document()
            self.configuration = self.doc.createElement('Configuration')
            self.doc.appendChild(self.configuration)

    def create_relation_item(self, node_id, browse_name):
        if self.get_relation_item(node_id, browse_name) is not None:
            return
        item = self.doc.createElement('relation')
        node_id_attr = self.get_node_id_attr(node_id)
        item.setAttribute('NodeId', node_id_attr)
        item.setAttribute('BrowseName', browse_name)
        self.configuration.appendChild(item)

    def add_child_item(self, parent_id, parent_name, child_id, child_name):
        relation_item = self.get_relation_item(parent_id, parent_name)
        if self.get_child_item(relation_item, child_id, child_name) is not None:
            return
        child = self.doc.createElement('child')
        child_id_attr = self.get_node_id_attr(child_id)
        child.setAttribute('NodeId', child_id_attr)
        child.setAttribute('BrowseName', child_name)
        relation_item.appendChild(child)

    def get_child_item(self, relation_item, node_id, browse_name):
        children = relation_item.getElementsByTagName("child")
        for child_item in children:
            nodeid = child_item.getAttribute("NodeId")
            browsename = child_item.getAttribute("BrowseName")
            if (self.get_node_id_attr(node_id) == nodeid) & (browse_name == browsename):
                return child_item

    def get_relation_item(self, node_id, browse_name):
        relations = self.doc.getElementsByTagName("relation")
        for relation in relations:
            nodeid = relation.getAttribute("NodeId")
            browsename = relation.getAttribute("BrowseName")
            if (self.get_node_id_attr(node_id) == nodeid) & (browse_name == browsename):
                return relation

    def get_node_id_attr(self, nodeid):
        return "ns=" + str(nodeid.NamespaceIndex) + ";i=" + str(nodeid.Identifier)

    def write_xml(self):
        f = open('model_set/' + self.xml_name, 'w')
        # f.write(doc.toprettyxml(indent = '\t', newl = '\n', encoding = 'utf-8'))
        self.doc.writexml(f, indent='\t', newl='\n', addindent='\t', encoding='utf-8')
        f.close()
