from xml.dom.minidom import Document, parse
import xml.sax


class XmlCreator:
    def __init__(self, xml_name):
        self.xml_name = xml_name
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
        child = self.doc.createElement('child')
        child_id_attr = self.get_node_id_attr(child_id)
        child.setAttribute('NodeId', child_id_attr)
        child.setAttribute('BrowseName', child_name)
        relation_item.appendChild(child)

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

    # def parse_xml(self):
    #     dom_tree = parse("uamodeler/relation.xml")
    #     collection = dom_tree.documentElement
    #     relations = collection.getElementsByTagName("relation")
    #     relation_mapping = {}
    #     for relation in relations:
    #         parentNodeId = relation.getAttribute('NodeId')
    #         children = relation.getElementsByTagName("child")
    #         childNodes = []
    #         for child in children:
    #             childNodeId = child.getAttribute("NodeId")
    #             childNodes.append(childNodeId)
    #         relation_mapping[parentNodeId] = childNodes
    #     return relation_mapping



# def create_xml(parent, child):
#     doc = Document()  # 创建DOM文档对象
#     document = doc.createElement('Configuration')  # 创建根元素
#     # document.setAttribute('content_method', "full")  # 设置命名空间
#     # DOCUMENT.setAttribute('xsi:noNamespaceSchemaLocation','DOCUMENT.xsd')#引用本地XML Schema
#     doc.appendChild(document)
#     # item:Python处理XML之Minidom
#     item = doc.createElement('relation')
#     item.setAttribute('Nodeid', 'ns=1;i=1')
#     item.setAttribute('BrowseName', 'parent')
#     document.appendChild(item)
#
#     # parent = doc.createElement('parent')
#     # parent.setAttribute('name', parent)
#     # parent_node = doc.createTextNode('ns=1;i=1')  # 元素内容写入
#     # parent.appendChild(parent_node)
#     # item.appendChild(parent)
#
#     child1 = doc.createElement('child')
#     child1.setAttribute('NodeId', 'ns=1;i=2')
#     child1.setAttribute('name', child)
#     # child1_node = doc.createTextNode('ns=1;i=2')
#     # child1.appendChild(child1_node)
#     item.appendChild(child1)
#
#     # child2 = doc.createElement('child')
#     # child2.setAttribute('name', "valve03")
#     # child2_node = doc.createTextNode('ns=1;i=3')
#     # child2.appendChild(child2_node)
#     # item.appendChild(child2)
#
#     # 将DOM对象doc写入文件
#     f = open('relation.xml', 'w')
#     # f.write(doc.toprettyxml(indent = '\t', newl = '\n', encoding = 'utf-8'))
#     doc.writexml(f, indent='\t', newl='\n', addindent='\t', encoding='utf-8')
#     f.close()


# def parse_xml():
#     # 使用minidom解析器打开xml文档
#     dom_tree = parse("relation.xml")
#     collection = dom_tree.documentElement
#
#     items = collection.getElementsByTagName("item")
#     for item in items:
#         parent = item.getElementsByTagName("parent")[0]
#         print("parent name:", parent.getAttribute("name"))
#         print("parent node:", parent.childNodes[0].data)
#
#         child = item.getElementsByTagName("child")[0]
#         print("child name:", child.getAttribute("name"))
#         print("child node:", child.childNodes[0].data)


# def parse_xml():
#     parser = xml.sax.make_parser()
#     # parser.setFeature(xml.sax.handler.feature_namespaces, 0)
#     handler = RelationHandler()
#     parser.setContentHandler(handler)
#     parser.parse("relation.xml")


# class RelationHandler(xml.sax.handler.ContentHandler):
#     def __init__(self):
#         self.CurrentData = ""
#         self.parent = ""
#
#     # 元素开始事件处理
#     def startElement(self, tag, attributes):
#         # tag xml标签
#         self.CurrentData = tag
#         if tag == "parent":
#             attr = attributes["name"]
#             print("parent name:", attr)
#
#     # 元素结束事件处理
#     def endElement(self, tag):
#         if self.CurrentData == "parent":
#             print("parent data:", self.parent)
#
#     # 内容事件处理
#     def characters(self, content):
#         if self.CurrentData == "parent":
#             self.parent = content


# if __name__ == "__main__":
    # create_xml('111', '222')
    # # parse_xml()
