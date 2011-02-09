"""
    SleekXMPP: The Sleek XMPP Library
    Copyright (C) 2010 Nathanael C. Fritz, Lance J.T. Stout
    This file is part of SleekXMPP.

    See the file LICENSE for copying permission.
"""

import logging
import copy

import sleekxmpp
from sleekxmpp import Message
from sleekxmpp.xmlstream.handler import Callback
from sleekxmpp.xmlstream.matcher import MatchXPath
from sleekxmpp.xmlstream import register_stanza_plugin, ElementBase, ET, JID
from sleekxmpp.plugins.base import base_plugin
from sleekxmpp.thirdparty import OrderedDict


log = logging.getLogger(__name__)


class Form(ElementBase):

    """
    """

    namespace = 'jabber:x:data'
    name = 'x'
    plugin_attrib = 'form'
    interfaces = set(('fields', 'instructions', 'items',
                      'reported', 'title', 'type', 'values'))
    sub_interfaces = set(('title',))
    form_types = set(('cancel', 'form', 'result', 'submit'))

    def __init__(self, *args, **kwargs):
        """
        """
        ElementBase.__init__(self, *args, **kwargs)
        self.field = OrderedDict()

        self.addField = self.add_field
        self.addReported = self.add_reported
        self.addItem = self.add_item
        self.setItems = self.set_items
        self.delItems = self.del_items
        self.getItems = self.get_items
        self.getInstructions = self.get_instructions
        self.setInstructions = self.set_instructions
        self.delInstructions = self.del_instructions
        self.getFields = self.get_fields
        self.setFields = self.set_fields
        self.delFields = self.del_fields
        self.getValues = self.get_values
        self.setValues = self.set_values
        self.getReported = self.get_reported
        self.setReported = self.set_reported
        self.delReported = self.del_reported

    def setup(self, xml=None):
        """
        """
        if ElementBase.setup(self, xml): #if we had to generate xml
            self['type'] = 'form'

    def add_field(self, var='', ftype=None, label='', desc='',
                        required=False, value=None, options=None, **kwargs):
        ftype = kwargs.get('type', ftype)

        field = FormField(parent=self)
        field['var'] = var
        field['type'] = ftype
        field['label'] = label
        field['desc'] = desc
        field['required'] = required
        field['value'] = value
        field['options'] = options

        self.field[var] = field

        return field

    def getXML(self, type='submit'):
        log.warning("Form.getXML() is deprecated API compatibility with plugins/old_0004.py")
        return self.xml

    def fromXML(self, xml):
        log.warning("Form.fromXML() is deprecated API compatibility with plugins/old_0004.py")
        n = Form(xml=xml)
        return n

    def add_item(self, values):
        item = ET.Element('{%s}item' % self.namespace)
        self.xml.append(item)
        for var in self['reported']:
            field_xml = ET.Element('{%s}field' % self.namespace)
            item.append(field_xml)
            field = FormField(xml=field_xml)
            field['var'] = var
            field['value'] = str(values.get(var, ''))

    def add_reported(self, var='', ftype=None, label='', desc='', **kwargs):
        ftype = kwargs.get('type', ftype)

        reported = self.xml.find('{%s}reported' % self.namespace)
        if reported is None:
            reported = ET.Element('{%s}reported' % self.namespace)
            self.xml.append(reported)

        field_xml = ET.Element('{%s}field' % self.namespace)
        reported.append(field_xml)

        field = FormField(xml=field_xml)
        field['var'] = var
        field['type'] = ftype
        field['label'] = label
        field['desc'] = desc

        return field

    def cancel(self):
        self['type'] = 'cancel'

    def del_fields(self):
        fields = self.xml.findall('{%s}field' % self.namespace)
        for field in fields:
            self.xml.remove(field)

    def del_instructions(self):
        instructions = self.xml.findall('{%s}instructions')
        for instruction in instructions:
            self.xml.remove(instruction)

    def del_items(self):
        items = self.xml.find('{%s}item' % self.namespace)
        for item in items:
            self.xml.remove(item)

    def del_reported(self):
        reported = self.xml.find('{%s}reported' % self.namespace)
        if reported is not None:
            self.xml.remove(reported)

    def get_fields(self, use_dict=False):
        fields = OrderedDict()
        fields_xml = self.xml.findall('{%s}field' % self.namespace)
        for field_xml in fields_xml:
            field = FormField(xml=field_xml)
            fields[field['var']] = field
        return fields

    def get_instructions(self):
        instructions = self.xml.findall('{%s}instructions' % self.namespace)
        return "\n".join([inst.text for inst in instructions])

    def get_items(self):
        items = []
        items_xml = self.xml.findall('{%s}item' % self.namespace)
        for item_xml in items_xml:
            item = OrderedDict()
            fields = item_xml.findall('{%s}field' % FormField.namespace)
            for field_xml in fields:
                field = FormField(xml=field_xml)
                item[field['var']] = field['value']
            items.append(item)
        return items

    def get_reported(self):
        reported = OrderedDict()
        fields = self.xml.findall('{%s}reported/{%s}field' % (
                                  self.namespace,
                                  self.namespace))
        for field_xml in fields:
            field = FormField(xml=field_xml)
            reported[field['var']] = field

        return reported

    def get_values(self):
        values = OrderedDict()
        fields = self.get_fields()
        for var in fields:
            values[var] = fields[var]['value']
        return values

    def reply(self):
        if self['type'] == 'form':
            self['type'] = 'submit'
        elif self['type'] == 'submit':
            self['type'] = 'result'

    def set_fields(self, fields, default=None):
        del self['fields']
        for var in fields:
            fields[var]['var'] = var
            self.add_field(**fields[var])

    def set_instructions(self, instructions):
        del self['instructions']
        if instructions not in [None, '']:
            instructions = instructions.split('\n')
            for instruction in instructions:
                inst = ET.Element('{%s}instructions' % self.namespace)
                inst.text = instruction
                self.xml.append(inst)

    def set_items(self, items):
        for item in items:
            self.add_item(item)

    def set_reported(self, reported):
        for var in reported:
            field = reported[var]
            field['var'] = var
            self.add_reported(**field)

    def set_values(self, values):
        fields = self.get_fields()
        for var in values:
            fields[var]['value'] = values[var]

    def merge(self, other):
        new = copy.copy(self)
        if type(other) == dict:
            new.setValues(other)
            return new
        nfields = new.get_fields()
        ofields = other.get_fields(use_dict=True)
        nfields.update(ofields)
        new.set_fields(nfields)
        return new


class FormField(ElementBase):
    namespace = 'jabber:x:data'
    name = 'field'
    plugin_attrib = 'field'
    interfaces = set(('answer', 'desc', 'required', 'value',
                      'options', 'label', 'type', 'var'))
    sub_interfaces = set(('desc',))
    field_types = set(('boolean', 'fixed', 'hidden', 'jid-multi',
                       'jid-single', 'list-multi', 'list-single',
                       'text-multi', 'text-private', 'text-single'))
    multi_value_types = set(('hidden', 'jid-multi',
                             'list-multi', 'text-multi'))
    multi_line_types = set(('hidden', 'text-multi'))
    option_types = set(('list-multi', 'list-single'))
    true_values = set((True, '1', 'true'))

    def addOption(self, label='', value=''):
        if self['type'] in self.option_types:
            opt = FieldOption(parent=self)
            opt['label'] = label
            opt['value'] = value
        else:
            raise ValueError("Cannot add options to a %s field." % self['type'])

    def delOptions(self):
        optsXML = self.xml.findall('{%s}option' % self.namespace)
        for optXML in optsXML:
            self.xml.remove(optXML)

    def delRequired(self):
        reqXML = self.xml.find('{%s}required' % self.namespace)
        if reqXML is not None:
            self.xml.remove(reqXML)

    def delValue(self):
        valsXML = self.xml.findall('{%s}value' % self.namespace)
        for valXML in valsXML:
            self.xml.remove(valXML)

    def getAnswer(self):
        return self.getValue()

    def getOptions(self):
        options = []
        optsXML = self.xml.findall('{%s}option' % self.namespace)
        for optXML in optsXML:
            opt = FieldOption(xml=optXML)
            options.append({'label': opt['label'], 'value':opt['value']})
        return options

    def getRequired(self):
        reqXML = self.xml.find('{%s}required' % self.namespace)
        return reqXML is not None

    def getValue(self):
        valsXML = self.xml.findall('{%s}value' % self.namespace)
        if len(valsXML) == 0:
            return None
        elif self['type'] == 'boolean':
            return valsXML[0].text in self.true_values
        elif self['type'] in self.multi_value_types:
            values = []
            for valXML in valsXML:
                if valXML.text is None:
                    valXML.text = ''
                values.append(valXML.text)
            if self['type'] == 'text-multi':
                values = "\n".join(values)
            return values
        else:
            return valsXML[0].text

    def setAnswer(self, answer):
        self.setValue(answer)

    def setFalse(self):
        self.setValue(False)

    def setOptions(self, options):
        self.delOptions()
        if options is not None:
            for value in options:
                if isinstance(value, dict):
                    self.addOption(**value)
                else:
                    self.addOption(value=value)

    def setRequired(self, required):
        exists = self.getRequired()
        if not exists and required:
            self.xml.append(ET.Element('{%s}required' % self.namespace))
        elif exists and not required:
            self.delRequired()

    def setTrue(self):
        self.setValue(True)

    def setValue(self, value):
        self.delValue()
        valXMLName = '{%s}value' % self.namespace

        if self['type'] == 'boolean':
            if value in self.true_values:
                valXML = ET.Element(valXMLName)
                valXML.text = '1'
                self.xml.append(valXML)
            else:
                valXML = ET.Element(valXMLName)
                valXML.text = '0'
                self.xml.append(valXML)
        elif self['type'] in self.multi_value_types or self['type'] in ['', None]:
            if self['type'] in self.multi_line_types and isinstance(value, str):
                value = value.split('\n')
            if not isinstance(value, list):
                value = [value]
            for val in value:
                if self['type'] in ['', None] and val in self.true_values:
                    val = '1'
                valXML = ET.Element(valXMLName)
                valXML.text = val
                self.xml.append(valXML)
        else:
            if isinstance(value, list):
                raise ValueError("Cannot add multiple values to a %s field." % self['type'])
            valXML = ET.Element(valXMLName)
            valXML.text = value
            self.xml.append(valXML)


class FieldOption(ElementBase):
    namespace = 'jabber:x:data'
    name = 'option'
    plugin_attrib = 'option'
    interfaces = set(('label', 'value'))
    sub_interfaces = set(('value',))


class xep_0004(base_plugin):
    """
    XEP-0004: Data Forms
    """

    def plugin_init(self):
        self.xep = '0004'
        self.description = 'Data Forms'

        self.xmpp.register_handler(
            Callback('Data Form',
                 MatchXPath('{%s}message/{%s}x' % (self.xmpp.default_ns,
                                   Form.namespace)),
                 self.handle_form))

        register_stanza_plugin(FormField, FieldOption)
        register_stanza_plugin(Form, FormField)
        register_stanza_plugin(Message, Form)

    def makeForm(self, ftype='form', title='', instructions=''):
        f = Form()
        f['type'] = ftype
        f['title'] = title
        f['instructions'] = instructions
        return f

    def post_init(self):
        base_plugin.post_init(self)
        self.xmpp.plugin['xep_0030'].add_feature('jabber:x:data')

    def handle_form(self, message):
        self.xmpp.event("message_xform", message)

    def buildForm(self, xml):
        return Form(xml=xml)
