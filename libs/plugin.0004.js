/*
  Copyright 2010, Drakontas LLC
  Ilya Braude 
  ilya@drakontas.com
*/

/*
 * TODO:
 * - add support for 'desc' child of 'field'
 */

Strophe.addConnectionPlugin('dataform', {
    /*
     Extend connection object to have plugin name 'dataform'.
    */
    _conn: null,

	//The plugin must have the init function.
	init: function(conn) {

	    this._conn = conn;

	    /*
	      Function used to setup plugin.
	    */
	    
	    /* extend name space 
	     *  NS.X_DATA - XMPP Data Forms from XEP 0004.
	     *                 
	     *
	     */
        Strophe.addNamespace('X_DATA',
                             "jabber:x:data");
    },

    /**
     * Function: createForm
     *  
     * Create an XML node that represents a dataforms form
     *
     * 
     * Parameters:
     *  (String) form_type - The type of the form. Options are:
     *                       'form', 'submit', 'cancel', 'result'.
     * Returns:
     *    An XMLElement that represents an empty form. This XMLElement
     *    is also a dataform.mixins.DataForm (see API below).
     *
     */
    createForm: function(form_type){
        var form = $build("x", {xmlns: Strophe.NS.X_DATA, type: form_type});
        var formNode = form.tree();
        formNode = Strophe.Mixin.apply(formNode, this.mixins.DataForm);

        // throw in a pointer to this plugin
        formNode.dataform = this;
        
        return formNode;
    },
    
    /**
     * Function: formFieldToObject
     *  
     * Takes a form field as an XMLElement and turns it into
     * a JS Object serialized version (suitable for passing to setFields()).
     * 
     * Parameters:
     *  (XMLElement) fieldNode - The field XML node
     *
     * Returns:
     *    A javascript object.
     *
     */
    formFieldToObject: function(fieldNode){
        if( !fieldNode )
            return null;

        var field = {content: {}};

        var type = fieldNode.getAttribute("type");
        if( type ){
            field.type = type;
        }


        var label = fieldNode.getAttribute("label");
        if( label ){
            field.label = label;
        }

        var _var = fieldNode.getAttribute("var");
        if( _var ){
            field['var'] = _var;
        }
        

        var opts = null;
        Strophe.forEachChild(fieldNode, "option", function(option){
            try{
                if(!opts){
                    opts = {};
                }

                opts[option.getAttribute("label")] = Strophe.getText(option.getElementsByTagName("value")[0]);
            } catch (e){
                //problem parsing label/value, ignore
                Strophe.log(e);
            }
        });

        if( opts ){
            field.content.options = opts;
        }
            

        var vals = [];
        Strophe.forEachChild(fieldNode, "value", function(value){
            try{
                vals.push(Strophe.getText(value));
            } catch (e){
                //problem parsing value, ignore
                Strophe.log(e);
            }
        });

        if( vals.length == 1 ){
            vals = vals[0];
        }
        
        if( !(vals.length > 0) ){
            vals = "";
        }
        
        field.content.value = vals;


        var required = fieldNode.getElementsByTagName("required");
        if (required && required.length > 0){
            field.content.required = true;
        }

        return field;
    },

    /**
     * Function: formToFieldObjects
     *  
     * Takes a form as an XMLElement and turns it's fields it into
     * a JS Object serialized version (suitable for passing to setFields()).
     * NOTE: only the fields are serialized, instructions/title meta data are not.
     * 
     * Parameters:
     *  (XMLElement) fieldNode - The form XML node
     *
     * Returns:
     *    A javascript object containing all the fields in the form.
     *
     */
    formToFieldObjects: function(formNode){
        var fields = [];
        var form = this;

//        var fieldNodes = formNode.getElementsByName('field');
//        if(fieldNodes.length){
//        	for (var i=0; i<fieldNodes.length; i++){
//        		fields.push(form.formFieldToObject(field));
//        	}
//        }
        Strophe.forEachChild(formNode, "field", function(field){
            fields.push(form.formFieldToObject(field));
        });

        return fields;
    },

    mixins: {
        DataForm: {

            /**
             * Function: setTitle
             *  
             * Sets the title for the form. Overwrites title if it's present already.
             * Title is OPTIONAL.
             * 
             * Parameters:
             *  (String) title_text - Free-form text for the title.
             *
             * Returns:
             *    Itself (the form)
             *
             */
            setTitle: function(title_text){
                if( !title_text ){
                    return this;
                }

                var titles = this.getElementsByTagName("title");
                var title;

                if( titles.length > 0 ){
                    title = titles[0];
                } else {
                    title = Strophe.xmlElement("title",[]);
                    this.appendChild(title);
                }

                var text = Strophe.xmlTextNode(title_text);

                // cross-browser way to remove children
                while (title.hasChildNodes()){
	                title.removeChild(title.firstChild);
	            }

                title.appendChild(text);

                return this;
            },

            /**
             * Function: getTitle
             *  
             * Gets the title for the form or null if not present.
             * Title is OPTIONAL.
             * 
             * Returns:
             *    (String) The title for the form.
             *
             */
            getTitle: function(){
                var title = null;
                Strophe.forEachChild(this, 'title', function(t){
                    title = Strophe.getText(t);
                });

                return title;
            },


            /**
             * Function: setInstructions
             *  
             * Sets the instructions for the form. Overwrites  it's present already.
             * Instructions tag is OPTIONAL.
             * 
             * Parameters:
             *  (String) instructions_text - Free-form text for the instructions.
             *
             * Returns:
             *    Itself (the form)
             *
             */
            setInstructions: function(instructions_text){
                if( !instructions_text )
                    return this;

                var instructions_list = this.getElementsByTagName("instrutions");
                var instructions;

                if( instructions_list.length > 0 ){
                    instructions = instructions_list[o];
                } else {
                    instructions = Strophe.xmlElement("instructions",[]);
                    this.appendChild(instructions);
                }

                var text = Strophe.xmlTextNode(instructions_text);

                // cross-browser way to remove children
                while (instructions.hasChildNodes()){
	                instructions.removeChild(instructions.firstChild);
	            }

                instructions.appendChild(text);

                return this;
            },

            /**
             * Function: getInstructions
             *  
             * Gets the instructions for the form or null if not present.
             * Instructions is OPTIONAL.
             * 
             * Returns:
             *    (String) The instructions for the form.
             *
             */
            getInstructions: function(){
                var instructions = null;
                Strophe.forEachChild(this, 'instructions', function(i){
                    instructions = Strophe.getText(i);
                });

                return instructions;
            },


            /**
             * Function: setFormType
             *  
             * Sets the FORM_TYPE hidden field value.  This is required by most
             * submissions.
             * 
             * Parameters:
             *  (String) type_value - type value
             *
             * Returns:
             *    Itself (the form)
             *
             */
            setFormType: function(type_value){
                this.setFields([{type: "hidden",
                                 'var': "FORM_TYPE",
                                 content: {
                                     value: type_value}}]);
                
                return this;
            },

            /**
             * Function: getFormType
             *  
             * Gets the FORM_TYPE hidden field value.  
             * 
             * Parameters:
             *  (String) type_value - type value
             *
             * Returns:
             *    (String) The FORM_TYPE or null if it isn't set
             *
             */
            getFormType: function(){
                try{
                    var ft = this.getField("FORM_TYPE");
                    var vals = ft.getElementsByTagName("value");
                    var val = vals[0];
                    return Strophe.getText(val);

                    //return Strophe.getText(this.getField("FORM_TYPE").getElementsByTagName("value")[0]);
                }
                catch(e){
                    //console.log(e);
                    return null;
                }
            },


            /**
             * Function: setFields
             *  
             * Adds/sets arbitrary fields in the form.  If the forms already exist,
             * (as determined by a matching 'var') they are updated.
             * 
             * Parameters:
             *  (Object) fields - a list of fields, their properties and values.
             *                    For example, fields can be defined as such:
             * > fields = [{type: 'text-single',
             * >            label: 'The name of your bot',
             * >            'var': 'botname',
             * >            content: {value: 'My bot name',
             * >                      required: true}},
             * >           {type: 'list-multi',
             * >            label: 'Helpful description of your bot',
             * >            'var': 'description',
             * >            content: {value: ['news', 'contests'],
             * >                      options: {
             * >                       'Contests': 'contests', //label: value
             * >                       'News': 'news'}}}
             * >           ]
             *
             * NOTE - 'var' MUST be in quotes to not cause problems in chromium/others.
             * 
             * Returns:
             *    Itself (the form)
             *
             */
            setFields: function(fields){
                for(var i = 0; i < fields.length; i++){
                    // make sure this var doesn't already exist
                    var fieldNode = this.getField(fields[i]['var']);

                    // add the field if this var doesn't exist yet
                    if( !fieldNode ){
                        fieldNode = $build("field").tree();
                    }

                    this._setFieldNode(fieldNode, fields[i]);

                    this.appendChild(fieldNode);
                }

                return this;
            },

            /**
             * PrivateFunction: _setFieldNode
             *  
             * Sets a form field (i.e. an XML Node).  Makes sure it is empty first.
             * 
             * Parameters:
             *  (XMLElement) fieldNode - The "field" DOM node to apply field to
             *  (Object) field - The object that represents a field
             *
             * Returns:
             *    An XMLElement that represents the field or null if not found.
             *
             */
            _setFieldNode: function(fieldNode, field){
                var isList = function(obj){
                    try{
                        return obj.constructor.toString().match(/array/i) != null;
                    } catch(e){
                        return false;
                    }
                }


                // make sure fieldNode is empty
                while (fieldNode.hasChildNodes()){
	                fieldNode.removeChild(fieldNode.firstChild);
	            }
                // TODO: Should we clear attributes too?
                //         while (fieldNode.attributes.length > 0){
                // 	        //delete fieldNode.attributes[0];
                // 	    }

                // process the 'content'
                var content = field['content'];
                var fieldType = field['type'];
                if( content ){
                    if( content.value ){
                        // turn value into a list of values if it's not
                        var values = content.value;
                        if(!isList(values)){
                            if(fieldType === "text-multi"){
                                // separate value by newlines
                                values = values.split(/\n/);
                            } else {
                                values = [values];
                            }
                        }

                        for(var v = 0; v < values.length; v++){
                            fieldNode.appendChild($build('value').t(values[v]).tree());
                        }
                    }
                    
                    if( content.options ){
                        for(var option in content.options){
                            var opt = $build('option', {label: option})
                                .c('value').t(content.options[option]);

                            fieldNode.appendChild(opt.tree());
                        }
                    }

                    if( content.required && content.required === true ){
                        fieldNode.appendChild($build('required').tree());
                    }
                }
                
                // only attributes left (e.g., 'type', 'var', 'label')
                for (var k in field){  // basically Strophe.Builder.attrs:
                    if(field.hasOwnProperty(k) && k != 'content'){
                        fieldNode.setAttribute(k, field[k]);
                    }
                }

                return fieldNode;
            },

            /**
             * Function: getField
             *  
             * Gets a form field.  Returns the XMLElement for the field.
             * 
             * Parameters:
             *  (String) var_name - The 'var' attribute name of the form.
             *
             * Returns:
             *    An XMLElement that represents the field or null if not found.
             *
             */
            getField: function(var_name){
                if( !var_name )
                    return null;

                var fields = this.getElementsByTagName("field");
                for(var f = 0; f < fields.length; f++){
                    var field = fields[f];
                    if( field.getAttribute('var') == var_name ){
                        return field;
                    }
                }

                return null;
            },

            /**
             * Function: unserializeField
             *  
             * Takes a form field as an XMLElement and turns it into
             * a JS Object serialized version (suitable for passing to setFields()).
             * 
             * Parameters:
             *  (XMLElement) fieldNode - The field XML node
             *
             * Returns:
             *    A javascript object.
             *
             */
            unserializeField: function(fieldNode){
                if( !fieldNode )
                    return null;

                var field = {content: {}};

                var type = fieldNode.getAttribute("type");
                if( type ){
                    field.type = type;
                }


                var label = fieldNode.getAttribute("label");
                if( label ){
                    field.label = label;
                }

                var _var = fieldNode.getAttribute("var");
                if( _var ){
                    field['var'] = _var;
                }
                

                var opts = null;
                Strophe.forEachChild(fieldNode, "option", function(option){
                    try{
                        if(!opts){
                            opts = {};
                        }

                        opts[option.getAttribute("label")] = Strophe.getText(option.getElementsByTagName("value")[0]);
                    } catch (e){
                        //problem parsing label/value, ignore
                        Strophe.log(e);
                    }
                });

                if( opts ){
                    field.content.options = opts;
                }
                

                var vals = [];
                Strophe.forEachChild(fieldNode, "value", function(value){
                    try{
                        var value = Strophe.getText(value)

                        // do some type coersion
                        if( field.type == "boolean" ){
                            value = value.toLowerCase();
                            switch(value){
                            case 'false':
                            case '0':
                                value = false;
                                break;
                            default:
                                value = true;
                            }
                        } else if(!field.type) {
                            // if type is not available, seek out booleans anyway
                            switch(value){
                            case 'false':
                                value = false;
                                break;
                            case 'true':
                                value = true;
                            }
                        }
                        
                        vals.push(value);
                    } catch (e){
                        //problem parsing value, ignore
                        Strophe.log(e);
                    }
                });

                if(type === "text-multi"){
                    vals = vals.join('\n');
                }

                if( vals.length == 1 ){
                    vals = vals[0];
                }

                if( !(vals.length > 0) ){
                    vals = "";
                }
                
                field.content.value = vals;

                var required = fieldNode.getElementsByTagName("required");
                if (required && required.length > 0){
                    field.content.required = true;
                }

                return field;
            },

            /**
             * Function: unserialize
             *  
             * Turns this form's fields it into
             * a JS Object serialized version (suitable for passing to setFields()).
             * NOTE: only the fields are unserialized, instructions/title meta data are not.
             * 
             * Returns:
             *    An array of javascript objects representing all the fields in the form.
             *
             */
            unserialize: function(){
                var fields = [];
                var form = this;

                Strophe.forEachChild(form, "field", function(field){
                    fields.push(form.unserializeField(field));
                });

                return fields;
            },

            
            /**
             * Function: getReported
             *  
             * Turns this form's "reported" fields
             * The returned serialized version is suitable for passing to setReported().
             * 
             * Example return:
             * >  items = [
             * >      [ // reported field 1
             * >          {'var': 'field1',
             * >           label: 'field 1 label',
             * >           type: 'text-single'},
             * >      ],
             * >      [ // reported field 2
             * >          {'var': 'field2',
             * >           label: 'field 2 label',
             * >            type: 'text-single'},
             * >      ]
             * >  ]
             *
             * Returns:
             *    An array of javascript objects representing all the "reported" 
             *      fields in the form.
             *    Or empty array if no reported elements are found
             *
             */
            getReported: function(){
                return this.getItems("reported")[0] || [];
            },


            /**
             * Function: setReported
             *  
             * Sets the "reported" fields in this form to the the array of fields.
             * 
             * Parameters:
             *  (Array) fields - An array of fields. 
             *                  If 'fields' is undefined or null, it is treated as
             *                  an empty array - all current reported fields are cleared
             *
             * Example input:
             * >  items = [
             * >      // reported field 1
             * >      {'var': 'field1',
             * >       label: 'field 1 label',
             * >       type: 'text-single'},
             * >      
             * >      // reported field 2
             * >      {'var': 'field2',
             * >       label: 'field 2 label',
             * >        type: 'text-single'},
             * >  ]
             *
             * Returns:
             *    the form (this)
             *
             */
            setReported: function(fields){
                // 1. remove all "reported" fields
                var form = this;

                Strophe.forEachChild(form, "reported", function(reported){
                    reported.parentNode.removeChild(reported);
                });

                // update properties because we modified the tree above
                if(form._updateWrappedProperties)
                    form._updateWrappedProperties();

                // 2. set reported fields to the passed in fields
                fields = fields || [];
                if(fields.length > 0){
                    var reportedNode = $build("reported").tree();
                    form.insertBefore(reportedNode, form.firstChild);
                    
                    for(var j = 0; j < fields.length; j++){
                        var fieldNode = $build("field").tree();
                        form._setFieldNode(fieldNode, fields[j]);
                        reportedNode.appendChild(fieldNode);
                    }
                }

                return this;
            },


            /**
             * Function: getItems
             *  
             * Turns this form's items it into a 2D array of fields.
             * The returned serialized version is suitable for passing to setItems().
             * 
             * Example return:
             * >  items = [
             * >      [ // row/item 1
             * >          {'var': 'field1',
             * >           content: {value: 'Value 1'}},
             * >          {'var': 'field2',
             * >           content: {value: 'Value 1'}},
             * >      ],
             * >      [ // row/item 2
             * >          {'var': 'field1',
             * >           content: {value: 'Value 2'}},
             * >          {'var': 'field2',
             * >           content: {value: 'Value 2'}},
             * >      ]
             * >  ]
             *
             * Returns:
             *    An array of javascript objects representing all the fields in the form.
             *    Or empty array if no items are found
             *
             */
            getItems: function(elemName){
                var items = [];
                var form = this;

                elemName = elemName || "item";

                Strophe.forEachChild(form, elemName, function(item){
                    var fields = [];
                    Strophe.forEachChild(item, "field", function(field){
                        fields.push(form.unserializeField(field));
                    });
                    items.push(fields);
                });

                return items;
            },

            /**
             * Function: setItems
             *  
             * Sets the items in this form to the a 2D array of fields.
             * 
             * Parameters:
             *  (Array) items - A 2D array of fields (item rows). 
             *                  If items is undefined or null, it is treated as
             *                  an empty array - all current items are cleared
             *
             * Example input:
             * >  [
             * >      [ // row/item 1
             * >          {'var': 'field1',
             * >           content: {value: 'Value 1'}},
             * >          {'var': 'field2',
             * >           content: {value: 'Value 1'}},
             * >      ],
             * >      [ // row/item 2
             * >          {'var': 'field1',
             * >           content: {value: 'Value 2'}},
             * >          {'var': 'field2',
             * >           content: {value: 'Value 2'}},
             * >      ]
             * >  ]
             *
             * Returns:
             *    the form (this)
             *
             */
            setItems: function(items){
                // 1. remove all items
                var form = this;

                var existing_items = [];
                Strophe.forEachChild(form, "item", function(item){
                    existing_items.push(item);
                });

                for(var i = 0; i < existing_items.length; i++){
                    existing_items[i].parentNode.removeChild(existing_items[i]);
                }

                // 2. set items to the passed in items
                items = items || [];
                for(var i = 0; i < items.length; i++){
                    var fields = items[i] || [];
                    var itemNode = $build("item").tree();
                    form.appendChild(itemNode);
                    for(var j = 0; j < fields.length; j++){
                        var fieldNode = $build("field").tree();
                        form._setFieldNode(fieldNode, fields[j]);
                        itemNode.appendChild(fieldNode);
                    }
                }

                return this;
            },


            /**
             * Function: getDOM
             * 
             * Returns the actual XML DOM Element for this form.
             * This function is here for future forward compatability with
             * an IE mixin workaround
             * 
             * Returns:
             *    (XMLElement) A valid XML DOM Element (that can be passed to $buid().cnode())
             */
            getDOM: function(){
                return this.dom ? this.dom : this;
            }
        }
    }
});