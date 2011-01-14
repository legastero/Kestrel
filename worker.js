var Worker = {
    bosh_url: 'http://lancestout.com:5280/http-bind',
    connection: null,
    max_tasks: 1,
    process: function (cmd, callback) {
        if (cmd !== '') {
            alert("Working on "+cmd+"....");
        }
        Worker.log("Command completed: " + cmd);
        callback();
    },
    tasks: {},
    log: function (msg) {
        $('#log').append('<li>'+msg+'</li>');
    },
    do_connect: function () {
        $('#login_dialog').dialog({
            autoOpen: true,
            draggable: true,
            modal: true,
            title: 'Connect to Server',
            buttons: {
                "Connect": function () {
                    $(document).trigger('connect', {
                        jid: $('#jid').val(),
                        password: $('#password').val(),
                    });

                    $('#password').val('');
                    $(this).dialog('close');
                }
            }
        });
    },
    on_connect: function () {
        Worker.log("Connection established.");
        Worker.connection.addHandler(Worker.on_disco_info, 
                                     Strophe.NS.DISCO_INFO,
                                     "iq");
        Worker.connection.addHandler(Worker.on_disco_items, 
                                     Strophe.NS.DISCO_ITEMS,
                                     "iq");

        Worker.connection.adhoc.onCommand('run_task', 'cancel', 
                                          Worker.on_run_task_cancel);
        Worker.connection.adhoc.onCommand('run_task', 'complete', 
                                          Worker.on_run_task_complete);
        Worker.connection.adhoc.onCommand('run_task', 'next', 
                                          Worker.on_run_task_next);
        Worker.connection.adhoc.onCommand('run_task', 'execute', 
                                          Worker.on_run_task_execute);
    },
    on_run_task_execute: function (iq) {
        Worker.log("Task request received");
        var resp = $iq({
            to: $(iq).attr('from'),
            type: 'result',
            id: $(iq).attr('id')});
        var datetime = new Date();
        var sessid = datetime.getTime() + $(iq).attr('id');
        resp.c("command", {xmlns: Strophe.NS.ADHOC,
                           status: 'executing',
                           sessionid: sessid,
                           node: $(iq).find('command').attr('node')});
        resp.c("actions", {action: 'next'}).c("next").up().up();
        resp.c("x", {xmlns: "jabber:x:data", type: 'form'});
        resp.c("field", {'var': 'command', 
                         type: 'text-single', 
                         label: 'Command'});
        resp.c("required");
        Worker.connection.send(resp);

        return true;
    },
    on_run_task_next: function (iq) {
        var command = $(iq).find('x').find('field').find('value').text();
        Worker.log("Task command received: " + command);
        Worker.process(command, function () {
            var resp = $iq({
                to: $(iq).attr('from'),
                type: 'result',
                id: $(iq).attr('id')});
            resp.c("command", {
                xmlns: Strophe.NS.ADHOC,
                status: 'executing',
                sessionid: $(iq).find('command').attr('sessionid'),
                node: $(iq).find('command').attr('node')});
            resp.c("actions", {action: 'complete'});
            resp.c("complete").up().up();
            resp.c("x", {xmlns: "jabber:x:data", type: 'form'});
            resp.c("field", {'var': 'cleanup', 
                             type: 'text-single', 
                             label: 'Cleanup'});
            resp.c("required");
            Worker.connection.send(resp);
        });

        return true;
    },
    on_run_task_complete: function (iq) {
        var command = $(iq).find('x').find('field').find('value').text();
        Worker.log("Task cleanup received: " + command);
        Worker.process(command, function () {
            var resp = $iq({
                to: $(iq).attr('from'),
                type: 'result',
                id: $(iq).attr('id')});
            resp.c("command", {
                xmlns: Strophe.NS.ADHOC,
                status: 'completed',
                sessionid: $(iq).find('command').attr('sessionid'),
                node: $(iq).find('command').attr('node')});
            Worker.connection.send(resp);
        });
        return true;
    },
    on_run_task_cancel: function (iq) {
        Worker.log("Task request canceled");
        var resp = $iq({
            to: $(iq).attr('from'),
            type: 'result',
            id: $(iq).attr('id')});
        resp.c("command", {
            xmlns: Strophe.NS.ADHOC,
            status: 'completed',
            sessionid: $(iq).find('command').attr('sessionid'),
            node: $(iq).find('command').attr('node')});

        return true;
    },
    on_disconnect: function () {
        Worker.log("Connection terminated.");
        Worker.connection = null;
    },
    on_disco_info: function (iq) {
        var resp = $iq({
            to: $(iq).attr('from'), 
            type: "result",
            id: $(iq).attr('id')});
        q = resp.c("query", {xmlns: Strophe.NS.DISCO_INFO});
        if ($(iq).find('query').attr('node') === Strophe.NS.ADHOC) {
            q.c("identity", {category: 'automation', 
                             type: 'command-list', 
                             name: 'Ad-hoc Commands'});
        }
        else if ($(iq).find('query').attr('node') === 'run_task') {
            q.c("identity", {category: 'automation', 
                             type: 'command-node', 
                             name: 'Run Kestrel Task'});
        }
        else {
            q.c("identity", {category: 'client', 
                             type: 'bot', 
                             name: 'Kestrel Web Worker'}).up()
            q.c("feature", {'var': Strophe.NS.DISCO_INFO});
            q.up();
            q.c("feature", {'var': Strophe.NS.ADHOC});
        }
        Worker.connection.send(resp);

        return true;
    },
    on_disco_items: function (iq) {
        var resp = $iq({
            to: $(iq).attr('from'), 
            type: "result",
            id: $(iq).attr('id')});
        q = resp.c("query", {xmlns: Strophe.NS.DISCO_ITEMS});
        if ($(iq).find('query').attr('node') === Strophe.NS.ADHOC) {
            q.c("item", {jid: $(iq).attr('to'), 
                         node: 'run_task', 
                         name: 'Run Kestrel Task'});
        }
        else if ($(iq).find('query').attr('node') === 'run_task') { 
            // No items
        }
        else {
            q.c("item", {jid: $(iq).attr('to'), 
                         node: Strophe.NS.ADHOC, 
                         name: 'Ad-Hoc Commands'});
        }
        Worker.connection.send(resp);
     
        return true;
    },
}

var XMPPConsole = {
    connection: null,
    pretty_xml: function (xml, level) {
        var i, j;
        var result = [];
        if (!level) {
            level = 0;
        }

        result.push("<div class='xml_level" + level + "'>");
        result.push("<span class='xml_punc'>&lt;</span>");
        result.push("<span class='xml_tag'>");
        result.push(xml.tagName);
        result.push("</span>");

        var attrs = xml.attributes;
        var attr_lead = [];
        for (i = 0; i < xml.tagName.length + 1; i++) {
            attr_lead.push("&nbsp;");
        }
        attr_lead = attr_lead.join("");

        for (i = 0; i < attrs.length; i++) {
            result.push(" <span class='xml_aname'>");
            result.push(attrs[i].nodeName);
            result.push("</span>");
            result.push("<span class='xml_punc'>='</span>");
            result.push(attrs[i].nodeValue);
            result.push("</span>");
            result.push("<span class='xml_punc'>'</span>");

            if (i !== attrs.length - 1) {
                result.push("</div>");
                result.push("<div class='xml_level" + level + "'>");
                result.push(attr_lead);
            }
        }

        if (xml.childNodes.length === 0) {
            result.push("<span class='xml_punc'> /&gt;</span>");
            result.push("</div>");
        } else {
            result.push("<span class='xml_punc'>&gt;</span>");
            result.push("</div>");

            $.each(xml.childNodes, function () {
                if (this.nodeType === 1) {
                    result.push(XMPPConsole.pretty_xml(this, level + 1));
                } else if (this.nodeType === 3) {
                    result.push("<div class='xml_text xml_level" + (level + 1) + "'>");
                    result.push(this.nodeValue);
                    result.push("</div>");
                }
            });

            result.push("<div class='xml_level" + level + "'>");
            result.push("<span class='xml_punc'>&lt;/</span>");
            result.push("<span class='xml_tag'>");
            result.push(xml.tagName);
            result.push("</span>");
            result.push("<span class='xml_punc'>&gt;</span>");
            result.push("</div>");
        }

        return result.join("");
    },

    show_xml: function (body, type) {
        if (body.childNodes.length > 0) {
            var console = $('#xmpp_console').get(0);
            var at_bottom = console.scrollTop >= console.scrollHeight - console.clientHeight;
            $.each(body.childNodes, function () {
                $('#xmpp_console').append(
                    "<div class='"+type+"'>" + 
                      XMPPConsole.pretty_xml(this) + 
                    "</div>");
            });

            if (at_bottom) {
                console.scrollTop = console.scrollHeight;
            }
        }
    },
};

$(document).ready(function() {
    $('#connection_button').click(function () {
        if (Worker.connection === null) {
            Worker.do_connect();
        }
        else {
            Worker.connection.disconnect();
        }
    });
    $('#control_panel').tabs({fx: {opacity: 'toggle'}});
    $('.button').button();
    Worker.do_connect();
});

$(document).bind('connect', function (ev, data) {
    var conn = new Strophe.Connection(Worker.bosh_url);

    conn.xmlInput = function (body) {
        XMPPConsole.show_xml(body, 'incoming');
    };

    conn.xmlOutput = function (body) {
        XMPPConsole.show_xml(body, 'outgoing');
    };

    conn.connect(data.jid, data.password, function (status) {
        if (status === Strophe.Status.CONNECTED) {
            $(document).trigger('connected');
        } else if (status === Strophe.Status.DISCONNECTED) {
            $(document).trigger('disconnected');
        }
    });

    Worker.connection = conn;
    XMPPConsole.connection = conn;
});

$(document).bind('connected', function () {
    Worker.on_connect();
    $('#connection_button').attr('value', 'Disconnect');
});

$(document).bind('disconnected', function () {
    Worker.on_disconnect();
    $('#connection_button').attr('value', 'Connect');
});
