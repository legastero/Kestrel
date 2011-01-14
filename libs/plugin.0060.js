/*
  Copyright 2010, Drakontas LLC
  Ilya Braude ilya@drakontas.com

  Original version Copyright 2008, Stanziq  Inc.
*/
(function($sp, callback){

var pubsub = {
/*
  Extend connection object to have plugin name 'pubsub'.  
*/
    _conn: null,

    //The plugin must have the init function.
    init: function(conn) {

        this._conn = conn;
        this.util._conn = conn;
        this.owner._conn = conn;

        /*
          Function used to setup plugin.
        */
        
        /* extend name space 
         *  NS.PUBSUB - XMPP Publish Subscribe namespace
         *              from XEP 60.  
         *
         *  NS.PUBSUB_SUBSCRIBE_OPTIONS - XMPP pubsub
         *                                options namespace from XEP 60.
         */
        Strophe.addNamespace('PUBSUB',"http://jabber.org/protocol/pubsub");
        Strophe.addNamespace('PUBSUB_SUBSCRIBE_OPTIONS',
                             Strophe.NS.PUBSUB+"#subscribe_options");
        Strophe.addNamespace('PUBSUB_ERRORS',Strophe.NS.PUBSUB+"#errors");
        Strophe.addNamespace('PUBSUB_EVENT',Strophe.NS.PUBSUB+"#event");
        Strophe.addNamespace('PUBSUB_OWNER',Strophe.NS.PUBSUB+"#owner");
        Strophe.addNamespace('PUBSUB_AUTO_CREATE',
                             Strophe.NS.PUBSUB+"#auto-create");
        Strophe.addNamespace('PUBSUB_PUBLISH_OPTIONS',
                             Strophe.NS.PUBSUB+"#publish-options");
        Strophe.addNamespace('PUBSUB_NODE_CONFIG',
                             Strophe.NS.PUBSUB+"#node_config");
        Strophe.addNamespace('PUBSUB_CREATE_AND_CONFIGURE',
                             Strophe.NS.PUBSUB+"#create-and-configure");
        Strophe.addNamespace('PUBSUB_SUBSCRIBE_AUTHORIZATION',
                             Strophe.NS.PUBSUB+"#subscribe_authorization");
        Strophe.addNamespace('PUBSUB_GET_PENDING',
                             Strophe.NS.PUBSUB+"#get-pending");
        Strophe.addNamespace('PUBSUB_MANAGE_SUBSCRIPTIONS',
                             Strophe.NS.PUBSUB+"#manage-subscriptions");
        Strophe.addNamespace('PUBSUB_META_DATA',
                             Strophe.NS.PUBSUB+"#meta-data");
    },

    /***Function  
    
      Create a pubsub node on the given service with the given node
      name.

      Parameters:
      (String) service - The name of the pubsub service.
      (String) node -  The name of the pubsub node.
      (Dictionary) options - (optional) The configuration options for the  node.
      (Function) call_back - (optional) Used to determine if node
      creation was sucessful.

      Returns:
      Iq id used to send subscription.
    */
    createNode: function(service, node, options, call_back) {
        
        var iqid = this._conn.getUniqueId("pubsubcreatenode");
        
        var iq = $iq({to:service, type:'set', id:iqid});
        
        var form = this._conn.dataform.createForm('submit');
        form.setFormType(Strophe.NS.PUBSUB_NODE_CONFIG);
        if (options){
            for (var i in options)
            {
                var val = options[i];
                form.setFields([{'var': i, content: {value: val}}]);
            }
        }

        iq.c('pubsub',
             {xmlns:Strophe.NS.PUBSUB}).c('create',
                                          {node:node})
            .up().c('configure').cnode(form);


        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                call_back(response);
            }
            
            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  iqid,
                                  null);
        }

        this._conn.send(iq.tree());

        return iqid;
    },

    /***Function  
    
      Delete a pubsub node on the given service with the given node
      name.

      Parameters:
      (String) service - The name of the pubsub service.
      (String) node -  The name of the pubsub node.
      (Function) call_back - (optional) Used to determine if node
      deletion was sucessful.

      Returns:
      Iq id used to send subscription.
    */
    deleteNode: function(service, node, call_back) {
        
        var iqid = this._conn.getUniqueId("delete");
        
        var iq = $iq({to:service, type:'set', id:iqid});
        
        iq.c('pubsub',
             {xmlns:Strophe.NS.PUBSUB_OWNER}).c('delete',
                                                {node:node});

        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                call_back(response);
            }
            
            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  iqid,
                                  null);
        }

        this._conn.send(iq.tree());

        return iqid;
    },


    /***Function 
    Subscribe to a node in order to receive event items.

      Parameters:
      (String) service - The name of the pubsub service.
      (String) node -  The name of the pubsub node.
      (Dictionary) options -  The configuration options for the  node.
      (Function) event_cb - (optional) Used to recieve subscription events.
      (Function) call_back - (optional) Used to determine if node
      creation was sucessful.
      (String) jid - (optional) If not specified, JID of current connection
      will be used.

      Returns:
      Iq id used to send subscription.
    */
    subscribe: function(service, node, options, event_cb, call_back, jid) {
        
        var reqId = this._conn.getUniqueId("subscribenode");
        
        if( !jid )
            jid = this._conn.jid; // or should we use the bare jid?

        var sub = $iq({from:jid, to:service, type:'set', id:reqId});
        sub.c('pubsub',
              {xmlns:Strophe.NS.PUBSUB})
            .c('subscribe',
               {node: node, jid: jid});

        if( options ){
            //create subscription options
            var form = this._conn.dataform.createForm('submit');
            form.setFormType(Strophe.NS.PUBSUB_SUBSCRIBE_OPTIONS);

            for (var i in options)
            {
                var val = options[i];
                form.setFields([{'var': i, content: {value: val}}]);
            }

            sub.up().c('options').cnode(form);
        }
        

        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                
                if( event_cb ){
                    // add notification listener on successful subscription
                    if( response.getType() == "result" ){
                        pubsub._conn.pubsub.addNotificationHandler(service, node, event_cb);
                    }
                }
                    
                call_back(response);
            }

            
            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  reqId,
                                  null);
        }
        this._conn.send(sub.tree());

        return reqId;
        
    },
    /***Function
    Unsubscribe from a node.

      Parameters:
      (String) service - The name of the pubsub service.
      (String) node -  The name of the pubsub node.
      (Function) call_back - (optional) Used to determine if node
      creation was sucessful.
      (String) jid - (optional) If not specified, JID of current connection
      will be used.
      (String) subid -  (optional) The user's subscription id for this node (required by OpenFire)
    
    */
    unsubscribe: function(service, node, call_back, jid, subid) {
        
        var reqId = this._conn.getUniqueId("unsubscribenode");

        if( !jid )
            jid = this._conn.jid;  // or should we use the bare jid?

        var sub = $iq({from:jid, to:service, type:'set', id:reqId});
        sub.c('pubsub', { xmlns:Strophe.NS.PUBSUB }).c('unsubscribe',
                                                       {node:node,
                                                        jid:jid});
        if(subid){
            sub.attrs({subid: subid});
        }


        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                call_back(response);
            }

            
            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  reqId,
                                  null);
        }

        this._conn.send(sub.tree());
        
        return reqId;
        
    },


    /***Function: getSubscriptionOptions
     Get current node configuration options.

       Parameters:
       (String) service - The name of the pubsub service.
       (String) node -  The name of the pubsub node.
       (Function) call_back - (optional) Returns the server response.
        Call .getForm() to get the data form with options.
       (String) jid - (optional) If not specified, JID of current connection
       (String) subid - (optional) Subid for the subscription

       Returns:
       Iq id used to send request.
     */
    getSubscriptionOptions: function(service, node, call_back, jid, subid){
        var iqid = this._conn.getUniqueId("get_options");
        
        var iq = $iq({to:service, type:'get', id:iqid});

        if( !jid )
            jid = this._conn.jid; // or should we use the bare jid?

        iq.c('pubsub',
             {xmlns:Strophe.NS.PUBSUB})
            .c('options', {node:node, jid: jid});

        if( subid ){
            iq.attrs({subid: subid});
        }
        
        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);

                // if the dataform plugin is loaded, use it
                var dataform = pubsub._conn.dataform;
                if(dataform){
                    response = Strophe.Mixin.apply(response, {
                        getForm: function(){
                            var form = $sp(this).find("options > x").get(0);
                            if(form){
                                return Strophe.Mixin.apply(form,
                                                           dataform.mixins.DataForm);
                            }
                        }
                    });
                }

                call_back(response);
            }
            
            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  iqid,
                                  null);
        }

        this._conn.send(iq.tree());

        return iqid;
    },

    /***Function 
    
      Publish and item to the given pubsub node.

      Parameters:
      (String) service - The name of the pubsub service.
      (String) node -  The name of the pubsub node.
      (List[XMLNode]) payloads -  The payload to be published (may be a list of payloads or a single payload).
      (List[String]) itemids -  (optional) The item id to use (may be a list of item ids or a single itemid).
      (Function) call_back - (optional) Used to determine if publish
       was sucessful.
    */    
    publish: function(service, node, payloads, itemids, call_back) {
        var pubid = this._conn.getUniqueId("publishnode");
        
        var pub = 
            $iq({to:service, type:'set', id:pubid})
            .c('pubsub', { xmlns:Strophe.NS.PUBSUB })
            .c("publish", {node: node});


        if( !Strophe.Util.isArray(payloads) ){
            payloads = [payloads];
        }

        if( !itemids ) {
            itemids = [];
        }
        else if( !Strophe.Util.isArray(itemids) ){
            itemids = [itemids];
        }

        for(var i = 0; i < payloads.length; i++){
            var item =  $build("item");

            var itemid = itemids[i];
            if( itemid != null && item != undefined ){
                item.attrs({id: itemid});
            }

            var payload = payloads[i];
            if( payload ){
                item.cnode(payload.tree());
            }

            pub.cnode(item.tree()).up();
        }

        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                call_back(response);
            }

            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  pubid,
                                  null);
        }

        this._conn.send(pub.tree());

        return pubid;
    },

    /***Function 
    
      Delete an item from the given pubsub node.

      Parameters:
      (String) service - The name of the pubsub service.
      (String) node -  The name of the pubsub node.
      (List[String]) itemids -  The itemid of the item to be deleted (may be a list of item ids or a single itemid).
      (Function) call_back - (optional) Used to determine if node
      creation was sucessful.
    */    
    retract: function(service, node, itemids, call_back) {
        var pubid = this._conn.getUniqueId("retract");
        
        var pub = 
            $iq({to:service, type:'set', id:pubid})
            .c('pubsub', { xmlns:Strophe.NS.PUBSUB })
            .c("retract", {node: node});


        if( !Strophe.Util.isArray(itemids) ){
            itemids = [itemids];
        }
        
        for(var i = 0; i < itemids.length; i++){
            pub.c("item", {id: itemids[i]}).up();
        }
         

        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                call_back(response);
            }

            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  pubid,
                                  null);
        }

        this._conn.send(pub.tree());

        return pubid;
    },

    /***
    Function discoverItems 
    
    Discover items on a specific node.

    The returned items will have a special 'id' field (copied from item.name)
    so that it makes more sense in the pubsub context of disco.

    Parameters:
    (String) service - The name of the pubsub service.
    (String) node -  The name of the pubsub node.
    (Function) call_back - (optional) Used to determine if the request was successful
    */
    discoverItems: function(service, node, call_back){
        var callback_wrapper = function(response){
            // also symlink item.name to item.id b/c in the
            // pubsub context, item.id makes more sense
            if( call_back ){
                var items = response.getItems();
                if( items ){
                    for(var i = 0; i < items.length; i++){
                        items[i].id = items[i].name;
                    }
                }

                call_back(response);
            }
        }
        return this._conn.disco.discoverItems(service, node, callback_wrapper);
    },


    /***
    Function requestItems 
    
    Request items from a specific node

    Parameters:
    (String) service - The name of the pubsub service.
    (String) node -  The name of the pubsub node.
    (Function) call_back - (optional) Used to determine if the request was successful
    (String) subid -  (optional) The user's subscription id for this node (required by OpenFire)
    (Array) item_ids -  (optional) An Array of item ids to retrieve
    (int) max_items -  (optional) A specific number of items to request
   
    */
    requestItems: function(service, node, call_back, subid, item_ids, max_items){
        var reqId = this._conn.getUniqueId("requestItems"); // generate our unique id for this packet
        
        // create our IQ packet
        var req = $iq({to:service, type:'get', id:reqId});
        req.c('pubsub', { xmlns:Strophe.NS.PUBSUB });
        req.c('items', {node:node});
        
        // if a subId is supplied (required for OpenFire), use it
        if(subid){
            req.attrs({subid:subid});
        }
        
        // if a number of items is supplied, use it
        if(max_items){
            req.attrs({max_items:max_items});
        }
        
        // if an array of itemIds are supplied, use them
        if(item_ids){
            for(var i=0; i < item_ids.length; i++){
                // create an item subnode and set active node back to the "items" node
                req.c('item', {id:item_ids[i]}).up();
            }
        }
        
        
        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                // the response looks like a notification body
                // in an IQ stanza
                response = Strophe.Mixin.apply(response,
                                               pubsub.mixins.PubSub);
                call_back(response);
            }

            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  reqId,
                                  null);
        }

        this._conn.send(req.tree());
        return reqId;
    },
    
    /***
    Function requestSubscriptions 
    
    Request the subscriptions for a given node (for the current jid)

    Parameters:
    (String) service - The name of the pubsub service.
    (String) node -  The name of the pubsub node.    
    (Function) call_back - (optional) Used to determine if the request was successful
    */
    requestSubscriptions: function(service, node, call_back){
        var reqId = this._conn.getUniqueId("requestSubId"); // generate our unique id for this packet
        
        // generate our IQ packet
        var req = $iq({to:service, type:'get', id:reqId});
        req.c('pubsub', { xmlns:Strophe.NS.PUBSUB }); // add a <pubsub> element
                
        req.c('subscriptions');

        if( node ){
            // ** NOTE OpenFire ignores node attrib and returns subIds for all nodes
            req.attrs({node:node});
        }
        
        if( call_back ){
            var pubsub = this;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                call_back(response);
            }

            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  reqId,
                                  null);
        }
        this._conn.send(req.tree());            
    },


    /*** 
    Function requestNodes
    
    Request a list of the available pubsub nodes on the service

    Parameters:
    (String) service - The name of the pubsub service.        
    (Function) call_back - (optional) Used to do further processing with the list of nodes that are returned
  */  
    requestNodes : function(service, call_back){
        // make our service discovery call
        return this._conn.disco.discoverItems(service, null, call_back);
    },
    
    
    /*** 
    Function addNotificationHandler
 
    Adds a listener for notifications for the specified pubsub node.
    These notification listeners automatically renew themselves unless
    'oneoff' is set to 'true'.
    
    Parameters:
    (String) service - (optional) The pubsub service name
    (String) node - (optional) The pubsub node name
    (Function) call_back - Used to do further processing with the
    notification.  The argument to this callback is the full message
    stanza, with pubsub-specific functions
    (Boolean) oneoff - (optional) if set to 'true', handler will deregister itself
                       after firing.  (Defaults to 'false')
    (Boolean) nodup - NOT IMPLEMENTED (optional) if set to 'true', will not register handler there is already a handler with the same filter criteria.  (Defaults to 'false')
    */
    addNotificationHandler : function(service, node, call_back, oneoff, nodup){
        if( call_back ){
            oneoff = !!oneoff;
            nodup = !!nodup;
            var pubsub = this;
            var handler = this._conn.addHandler(function(elem){
                var handler_found = false;
                Strophe.forEachChild(elem, "event", function(event){
                    Strophe.forEachChild(event, null, function(event_elem){
                        // check 'items' and 'purge'
                        if( Strophe.isTagEqual(event_elem, "items") ||
                            Strophe.isTagEqual(event_elem, "purge") ){

                            if( !node || event_elem.getAttribute("node") == node ){
                                handler_found = true;
                                elem = Strophe.Mixin.apply(elem, pubsub.mixins.Notification);
                                handler_ret = call_back(elem);
                            }
                        }
                    });
                });

                if( handler_found && oneoff ){
                    return false; // do not want to continue
                } else {
                    return true;  // continue handling
                }
            }, Strophe.NS.PUBSUB_EVENT, 'message', null, null, service);
            
            return handler;
        }
    },

    deleteNotificationHandler: function(handler){
        this._conn.deleteHandler(handler);
    },

    /** Things that a node owner can do - in the pusbub#owner namespace */
    owner: {
        
        
        /***Function: owner.configure
        Configure a node with specific options.  Options are specified
          as a dictionary of name:value pairs.

          Parameters:
          (String) service - The name of the pubsub service.
          (String) node -  The name of the pubsub node.
          (Dictionary) options -  The configuration options for the  node.
          (Function) call_back - (optional) Used to determine if request
          was sucessful.

          Returns:
          Iq id used to send request.
        */
        configure: function(service, node, options, call_back){
            var iqid = this._conn.getUniqueId("configure");
            
            var iq = $iq({to:service, type:'set', id:iqid});

            var form = this._conn.dataform.createForm('submit');
            form.setFormType(Strophe.NS.PUBSUB_NODE_CONFIG);
            if (options){
                for (var i in options)
                {
                    var val = options[i];
                    form.setFields([{'var': i, content: {value: val}}]);
                }
            }
            
            iq.c('pubsub',
                 {xmlns:Strophe.NS.PUBSUB_OWNER})
                .c('configure',
                   {node:node}).cnode(form);

        
            if( call_back ){
                var pubsub = this._conn.pubsub;
                var callback_wrapper = function(response){
                    response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                    call_back(response);
                }
                
                this._conn.addHandler(callback_wrapper,
                                      null,
                                      'iq',
                                      null,
                                      iqid,
                                      null);
            }

            this._conn.send(iq.tree());

            return iqid;
        },

        /***Function 
        Get current node configuration options.

          Parameters:
          (String) service - The name of the pubsub service.
          (String) node -  The name of the pubsub node.
          (Function) call_back - (optional) Returns the server response.
           Call .getForm() to get the data form with options.

          Returns:
          Iq id used to send request.
        */
        getConfiguration: function(service, node, call_back){
            var iqid = this._conn.getUniqueId("get_configure");
            
            var iq = $iq({to:service, type:'get', id:iqid});

            iq.c('pubsub',
                 {xmlns:Strophe.NS.PUBSUB_OWNER})
                .c('configure', {node:node})
        
            if( call_back ){
                var pubsub = this._conn.pubsub;
                var callback_wrapper = function(response){
                    response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);

                    // if the dataform plugin is loaded, use it
                    var dataform = pubsub._conn.dataform;
                    if(dataform){
                        response = Strophe.Mixin.apply(response, {
                            getForm: function(){
                                var form = $sp(this).find("options > x").get(0);
                                if(form){
                                    return Strophe.Mixin.apply(form,
                                                               dataform.mixins.DataForm);
                                }
                            }
                        });
                    }

                    call_back(response);
                }
                
                this._conn.addHandler(callback_wrapper,
                                      null,
                                      'iq',
                                      null,
                                      iqid,
                                      null);
            }

            this._conn.send(iq.tree());

            return iqid;
        },

        /***Function 
        Get current node subcribers for the node.

          Parameters:
          (String) service - The name of the pubsub service.
          (String) node -  The name of the pubsub node.
          (Function) call_back - (optional) Returns the server response.

          Returns:
          Iq id used to send request.
        */
        getSubscriptions: function(service, node, call_back){
            var iqid = this._conn.getUniqueId("get_subscriptions");
            
            var iq = $iq({to:service, type:'get', id:iqid});

            iq.c('pubsub',
                 {xmlns:Strophe.NS.PUBSUB_OWNER})
                .c('subscriptions', {node:node})
        
            if( call_back ){
                var pubsub = this._conn.pubsub;
                var callback_wrapper = function(response){
                    response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                    call_back(response);
                }
                
                this._conn.addHandler(callback_wrapper,
                                      null,
                                      'iq',
                                      null,
                                      iqid,
                                      null);
            }

            this._conn.send(iq.tree());

            return iqid;
        },
            

        /***
        Function purge
        Purges all items from the node (may leave the last published item)

        Parameters:
        (String) service - The name of the pubsub service.
        (String) node -  The name of the pubsub node.
        (Function) call_back - Will be callsed with a single subid argument
        */
        purge: function(service, node, call_back){
            var iqid = this._conn.getUniqueId("purge");
            
            var iq = $iq({to:service, type:'set', id:iqid});
            
            iq.c('pubsub',
                 {xmlns:Strophe.NS.PUBSUB_OWNER}).c('purge',
                                                    {node:node});

            var pubsub = this._conn.pubsub;
            var callback_wrapper = function(response){
                response = Strophe.Mixin.apply(response, pubsub.mixins.PubSub);
                call_back(response);
            }
            
            this._conn.addHandler(callback_wrapper,
                                  null,
                                  'iq',
                                  null,
                                  iqid,
                                  null);
            this._conn.send(iq.tree());

            return iqid;
        }
    },

    util: {
        /***
        Function getSubId 

        Helper function to obtain a subid for the current session jid.
        For complete subscription discovery, use requestSubscriptions().

        Parameters:
        (String) service - The name of the pubsub service.
        (String) node -  The name of the pubsub node.
        (Function) call_back - Will be callsed with a single subid argument
        (String) jid -  [optional] A specific jid to match.
        */
        getSubId: function(service, node, call_back, jid){
            if (!jid)
                jid = this._conn.jid;
            
            var process_subs = function(resp){
                if( !call_back )
                    return;

                var subids = resp.getSubIds(node, jid, false);
                call_back(subids.length > 0 ? subids[0] : null);
            }

            // make request
            return this._conn.pubsub.requestSubscriptions(service, node, process_subs);
        },

        /***
        Function getSubIds 

        Helper function to obtain a subid list for the current session jid.
        For complete subscription discovery, use requestSubscriptions().

        Parameters:
        (String) service - The name of the pubsub service.
        (String) node -  The name of the pubsub node.
        (Function) call_back - Will be called with an array of subids
        (String) jid -  [optional] A specific jid to match.
        */
        getSubIds: function(service, node, call_back, jid){
            var exactMatch = true;

            if (!jid){
                jid = this._conn.jid;
                //exactMatch = false;
            }

            var process_subs = function(resp){
                if( !call_back )
                    return;

                call_back(resp.getSubIds(node, jid, exactMatch));
            }

            // make request
            return this._conn.pubsub.requestSubscriptions(service, node, process_subs);
        }
    },


    mixins: {

        /*
          Strophe.Packet.PubSub
          Functionality specific to a pubsub XMPP packets  
        */
        PubSub: Strophe.Mixin.apply({
            getItems: function(){
                return pubsub.mixins.Notification.getItems.apply(
                    this, ["pubsub > publish, items > item"]);
            },

            /** DEPRECATED! Use getItems instead **/
            getPubsubItems: function(){
                return this.getItems();
            },

            /**
             * Returns the node name for the pubsub stanza.
             */
            getNode: function(){
                return $sp(this).find("pubsub > publish, items, subscriptions").attr("node").get(0) || "";
            },

            /** DEPRECATED! Use getNode instead **/
            getSubscriptionsNode: function(){
                return this.getNode();
            },

            getSubscriptions : function(){
                var subElems = $sp(this).find("pubsub > subscriptions > subscription");
                var subscriptions = [];
                
                // shove the items into an array
                subElems.each(function(subElem){
                    var node = subElem.getAttribute('node') || null;
                    var jid = subElem.getAttribute('jid') || null;
                    var affiliation = subElem.getAttribute('affiliation') || null;
                    var subscription = subElem.getAttribute('subscription') || null;
                    var subid = subElem.getAttribute('subid') || null;
                    
                    subElem = Strophe.Mixin.apply(subElem, {
                        node: node,
                        jid: jid, 
                        affiliation: affiliation, 
                        subscription: subscription, 
                        subid: subid
                    });
                    
                    subscriptions.push(subElem);
                });

                return subscriptions;
            },
    
            getSubIds : function(node, jid, exactMatch){
                var exact_subids = [];
                var bare_subids = []
                
                if( this.getType() == "result" ){
                    var bareJid = Strophe.getBareJidFromJid(jid);
                    
                    var subs = this.getSubscriptions();
                    for(var i = 0; i < subs.length; i++){
                        var sub = subs[i];
                        if( sub.node && sub.node != node ){
                            continue;
                        }

                        if( sub.subid ){
                            if (sub.jid == jid){
                                exact_subids.push(sub.subid);
                            }
                            else if( !exactMatch && sub.jid == bareJid ){
                                bare_subids.push(sub.subid);
                            }
                        }
                    }
                }

                return exact_subids.concat(bare_subids);
            },
            
            /**
             * Returns the service that this stanza originated from
             */
            getService: function(){
                return this.getFrom();
            }
        }, Strophe.Mixin.IQ),

        
        Notification: Strophe.Mixin.apply({
            /**
             * Returns the node name for a notification.
             * Works for both 'items' and 'purge' notifications.
             */
            getNode: function(){
                return $sp(this).find("event > items, purge").attr("node").get(0) || "";
            },
    
            /**
             * Returns the list of published items in the notification.
             * Each item in the retunred array gets extra javascript properties:
             *  'id', 'payload', 'publisher', 'timestamp'
             */
            getItems : function(selector){
                selector = selector || "event, pubsub > items > item";

                var itemElems = $sp(this).find(selector);

                var pubSubItems = [];
                
                // shove the items in an array
                itemElems.each(function(item){
                    var id = item.getAttribute('id');
                    var payload = null;
                    if ((item.firstChild != undefined) && (item.firstChild != null)){
                        payload = item.firstChild;
                    }
                    
                    var publisher = item.getAttribute('publisher');
                    var timestamp = item.getAttribute('timestamp');
                    
                    item = Strophe.Mixin.apply(item,
                                               {
                                                   id: id, 
                                                   payload: payload,
                                                   publisher: publisher,
                                                   timestamp: timestamp
                                               });
                    
                    pubSubItems.push(item);
                });
                return pubSubItems;
            },

            /**
             * Returns the list of retracted items in the notification.
             * Each item in the retunred array has an attribute: 'id'
             */
            getRetractions : function(){
                return this.getItems("event > items > retract");
            },

            /**
             * Returns the purge element (at the same level as 'items') in the notification.
             */
            getPurge : function(){
                return this.getItems("event > purge");
            },
            
            /**
             * Returns the service that this notification originated from
             */
            getService: function(){
                return this.getFrom();
            }
        }, Strophe.Mixin.Message)
    }
}

if(callback){
    callback(pubsub);
}

})(Strophe.Parser, function(pubsub){
    Strophe.addConnectionPlugin('pubsub', pubsub);
});
