/*
 Copyright 2010, Drakontas LLC
 Kyle Usbeck kyle@drakontas.com
 */

Strophe.addConnectionPlugin('adhoc', {
    /*
     Extend the connection object to have an Ad-hoc Commands plugin a la XEP-0050
     */
    _conn: null,

    init: function(conn) {

        this._conn = conn;
        Strophe.addNamespace('ADHOC',"http://jabber.org/protocol/commands");
    },

    /***Function
     Sends an Ad-hoc command to a user and calls the callback on response

     Parameters:
     (String) to - Full JID to send the Ad-hoc command
     (String) node - Name of the command
     (String) action - Action to perform (e.g., execute, cancel)
     (Function) callback - handler callback for the response

     Returns:
     Iq id used to send the Ad-hoc command
     */
    sendCommand: function(to, node, action, callback) {
        // get an ID for the IQ stanza
        var iqid = this._conn.getUniqueId("exec");

        // no action -> execute
        if( !action ) {
            action = 'execute'
        }

        // make the XMPP message
        var iq = $iq({to: to, type: 'set', id: iqid});
        iq.c('command',
        {xmlns: Strophe.NS.ADHOC,
        node: node,
        action: action});

        // handle the response
        if( callback ) {
            this._conn.addHandler(callback,
            null,
            'iq',
            null,
            iqid,
            null);
        }

        // send the IQ
        this._conn.send(iq.tree());

        // return the ID of the IQ stanza
        return iqid;
    },

    /***Function
     Listens for an action on an Ad-hoc command node.  Specifying node or
     action filters the callback to matching node/action.

     Parameters:
     (String) node - Name of the command 
     (String) action - Action called (e.g., execute, cancel)
     (Function) callback - called with IQ stanza when the Ad-hoc command is received

     Returns:
     The Strophe handler registered to listen for the command (for purposes of listener cancellation)
     */
    onCommand: function(node, action, callback) {
        var that = this;
        var cmdCallback = function(stanza) {
            var iq = Strophe.Mixin.apply(stanza, that.mixins.Adhoc);
            if( iq.getType() !== 'error' ) {
                if( !node || (node && iq.getNode() === node) ) {
                    if( !action || (action && iq.getAction() === action) ) {
                        callback(iq);
                    }
                }
            }
        };
        return this._conn.addHandler(cmdCallback, //handler
                                     Strophe.NS.ADHOC, //NS (of direct child)
                                     'iq', //stanza name
                                     'set', //type
                                     null, //id
                                     null, //from
                                     null); //options
    },
});
