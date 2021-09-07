import React from 'react';
import './App.css';
//Bootstrap and jQuery libraries
import 'bootstrap/dist/css/bootstrap.min.css';
import 'jquery/dist/jquery.min.js';
//Datatable Modules
import "datatables.net-dt/js/dataTables.dataTables";
import "datatables.net-dt/css/jquery.dataTables.min.css";
import $ from 'jquery'; 
//For API Requests
import axios from 'axios';
class App extends React.Component {
  
  
  //Declare data store variables
  constructor(props) {
    super(props)
      this.state = {
        rows: [],
        cols:[]
              }
      }

 
  componentDidMount() {
    function insertAt(array, index, ...elementsArray) {
      array.splice(index, 0, ...elementsArray);
  };
    function moveArrayItemToNewIndex(arr, old_index, new_index) {
      if (new_index >= arr.length) {
          var k = new_index - arr.length + 1;
          while (k--) {
              arr.push(undefined);
          }
      }
      arr.splice(new_index, 0, arr.splice(old_index, 1)[0]);
      return arr; 
    };
    //array holders for rows(rowsholder) and column name holder(colsholder)
    const colsholder =[]
  
    const finalrowsholder=[]


    var finaleditedholder=[]

    var tempIDnameholder=[]
    var tempdatasetnameholder=[]
    var elements




    // on here, nid to make Python FASTAPI as middleware to bypass CORS, then axios.get(http://localhost/FASTAPI)
    axios.get('http://localhost:8000/getdatasets', {
      headers: {
          'X-RestLi-Protocol-Version': '2.0.0'
      }
    }).then(res => 
    { //pushing datasets data to 'elements' varaiable
      
      elements = (res["data"])
      let count =0
      // aspectSchemaMetadata=['aspects']+['com.linkedin.schema.SchemaMetadata']
      
    //For loop for all fields in dataset, compare with editableSchema fields; if exist, push both to first element of each array, thus index positions of both edited Schema
    // and Schemameta(original) will match 
    for(let i=0; i< elements.length; i++){
      for(let j=0; j< elements[i]["schemaMetadata"]["fields"].length; j++){
        if(elements[i]["editableSchemaMetadata"]!==undefined){
          for( let a = 0; a<elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"].length; a++){
            if(elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][a]["fieldPath"] === elements[i]["schemaMetadata"]["fields"][j]["fieldPath"]){
              moveArrayItemToNewIndex(elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"],a,0)
              moveArrayItemToNewIndex(elements[i]["schemaMetadata"]["fields"],j,0)
            }   
      }
    }
  }
}

//for loop for total datasets iteration
    for(let i=0; i< elements.length; i++){
      for(let j=0; j< elements[i]["schemaMetadata"]["fields"].length; j++){
        let rowsholder={}
        
        Object.assign(rowsholder,{"ID": count});
        count+=1
        //for loop for platform and table name of datasets, always add key and value pair when pushing to array so aDataSort can refrence later
        Object.assign(rowsholder,{"Origin": elements[i]["DatasetKey"]["origin"]});
        Object.assign(rowsholder, {"Platform_Name": elements[i]["DatasetKey"]["platform"].split(':').pop()});
        Object.assign(rowsholder,{"Dataset_Name": elements[i]["DatasetKey"]["name"]});
        
        //For elements with global tags, if they not equal to undefined, push the tags to array, else push ' ' to array
        if(elements[i]["GlobalTags"]!==undefined){
          let globaltagholder= []
        
          if(elements[i]["GlobalTags"]["tags"].length===0){
            globaltagholder.push(' ')
          }
          else{
          for(let k=0; k< elements[i]["GlobalTags"]["tags"].length; k++){
            
            if(k>0){
              globaltagholder.push(', '+ elements[i]["GlobalTags"]["tags"][k]["tag"].split(':').pop())
            }
            else{
        globaltagholder.push(elements[i]["GlobalTags"]["tags"][k]["tag"].split(':').pop())
      }
      }
      }
        Object.assign(rowsholder, ({"Global_Tags": globaltagholder}))
    
      }  else{
          let globaltagholder= []
          globaltagholder.push(' ')
          Object.assign(rowsholder, ({"Global_Tags": globaltagholder}))
  }
   
    //injest field name
      Object.assign(rowsholder,({"Field_Name": elements[i]["schemaMetadata"]["fields"][j]["fieldPath"]}))

      //if the dataset even has editableSchemadata
      if(elements[i]["editableSchemaMetadata"]!==undefined){
        //Field in editableSchemaMetadata has to match fields in schemaMetadata
        if(elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]!==undefined
        && elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["fieldPath"] === elements[i]["schemaMetadata"]["fields"][j]["fieldPath"])
        { let tagsholder= []
          if(elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"] !== undefined)
          {
          
          if(elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"].length ===0){
          tagsholder.push(' ')
        }else{
          for(let l=0; l< elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"].length; l++){
            if(l>0){
              tagsholder.push(', ' + (elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"][l]["tag"].split(':').pop()))
            }else{
            tagsholder.push((elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"][l]["tag"].split(':').pop()))
        }
      }
    }
    }
        Object.assign(rowsholder, ({"Editable_Tags": tagsholder}))
        Object.assign(rowsholder, ({"From_EditableSchema": "Yes"}))
        //If have editableschemametadata but fieldpaths dont match, set to NO
      }else{
        let tagsholder= []
        tagsholder.push(' ')
        Object.assign(rowsholder,({"Editable_Tags": tagsholder}))
        Object.assign(rowsholder, ({"From_EditableSchema": "No"}))
      } 
      //If do not have editableschemametadata at all
    }else{
      let tagsholder= []
      tagsholder.push(' ')
      Object.assign(rowsholder,({"Editable_Tags": tagsholder}))
      Object.assign(rowsholder, ({"From_EditableSchema": "No"}))
    }
       
      //Filling tags from schemametadata
      if(elements[i]["schemaMetadata"]["fields"][j]["globalTags"]!==undefined){
        let tagsholder= []
        if(elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"].length ===0){
          tagsholder.push(' ')
        }else{
        for(let m=0; m< elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"].length; m++){
            if(m>0){
          tagsholder.push(', ' + (elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"][m]["tag"].split(':').pop()))
         
        }else{
          tagsholder.push((elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"][m]["tag"].split(':').pop()))
      }
    }
  }
        Object.assign(rowsholder,({"Original_Tags": tagsholder}))
      }else{
        let tagsholder= []
        tagsholder.push(' ')
        Object.assign(rowsholder,({"Original_Tags": tagsholder}))
      }


      //Checks for Description in editableschemaMetaData first, then checks in SchemaMetaData.
      if(elements[i]["editableSchemaMetadata"]!==undefined){
        if (elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]!==undefined 
        && elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["fieldPath"] === elements[i]["schemaMetadata"]["fields"][j]["fieldPath"] 
        && elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["description"]!==undefined)
        { 
          Object.assign(rowsholder,({"Description": elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["description"]}))
         
      } else if(elements[i]["schemaMetadata"]["fields"][j]["description"]!==undefined)
      {
        Object.assign(rowsholder,({"Description": elements[i]["schemaMetadata"]["fields"][j]["description"]}))
      }
    }


    //Since already checked in editableschemaMetaData , now just checks in schemametadata, else if empty, fill with blank
      if (elements[i]["editableSchemaMetadata"] ===undefined && elements[i]["schemaMetadata"]["fields"][j]["description"]!==undefined){
            Object.assign(rowsholder,({"Description": elements[i]["schemaMetadata"]["fields"][j]["description"]}))

      }if (elements[i]["editableSchemaMetadata"] === undefined && elements[i]["schemaMetadata"]["fields"][j]["description"] === undefined){
        Object.assign(rowsholder,({"Description": ' '}))
      }
      //for Timestamp, checks if editableschemametadata exists, if not use schemametadata
      if(elements[i]["editableSchemaMetadata"] === undefined){
        let date = new Date (elements[i]["schemaMetadata"]["lastModified"]["time"])
        
        Object.assign(rowsholder,({ "Date_Modified": date.toLocaleString()}))
      }else{
        let date = new Date (elements[i]["editableSchemaMetadata"]["lastModified"]["time"])
        Object.assign(rowsholder,({ "Date_Modified": date.toLocaleString()}))
      }
      
      finalrowsholder.push(rowsholder)
      rowsholder = {}
      
    }
  
  }
  //Columns header defintion #important
    colsholder.push("#", "Platform_Name", "Dataset_Name","Global_Tags", "Field_Name", "Editable_Tags","Original_Tags", "Description", "Date_Modified","From_EditableSchema","Origin")
   
   
    
        
    console.log("Sorted fields of data retrived from GMS:",elements)
    console.log("Column Headers:",colsholder)
  
    console.log("Data to feed columns:",finalrowsholder)
    this.setState({rows: finalrowsholder, cols: colsholder});
       }); 
   
    //init Datatable, #example is the table element id
    setTimeout(()=>{                        
      var example =$('#example').DataTable(
        {order: [[ 0, "asc" ]],
          responsive: true,
       
       
          "lengthMenu": [[10, 20, 100, -1], [10, 20, 100, "All"]],
          columnDefs : [
            { "type": "html-input", targets: [3,5,6,7],
              render: function (rows, type, row) {
            
                return '<input class="form-control" type="text"  value ="'+ rows + '" style= "width:auto">';
              
              }
              
            }, {
              "targets": [0,9,10],
              "visible": false,
              "searchable": false
          }
          ]
          
        }
  
      )

 //Iterate thru all row and compare original data vs edited, if edited, add to array (finaleditedholder) to be sent to endpoint
  $('#test').click(function () {
    let editedrowsholder = {};
    finaleditedholder=[];
    example.rows().every(function(){
    
    
      if(this.data()[3] !== ($(example.cell(this.index(), 3).node()).find('input').val()) 
      ||this.data()[5] !== ($(example.cell(this.index(), 5).node()).find('input').val())
      ||this.data()[6] !== ($(example.cell(this.index(), 6).node()).find('input').val())
      ||this.data()[7] !== ($(example.cell(this.index(), 7).node()).find('input').val())
      ){
        let date = new Date();
        Object.assign(editedrowsholder,({"ID": parseInt(this.data()[0]),"Origin": this.data()[10], "Platform_Name": this.data()[1], "Dataset_Name": this.data()[2],
        "Global_Tags": ($(example.cell(this.index(), 3).node()).find('input').val()), "Field_Name": this.data()[4], 
        "Editable_Tags": ($(example.cell(this.index(), 5).node()).find('input').val()),
        "Original_Tags": ($(example.cell(this.index(), 6).node()).find('input').val()),
        "Description": ($(example.cell(this.index(), 7).node()).find('input').val()), "Date_Modified": Date.parse(date.toLocaleString())}))
        finaleditedholder.push(editedrowsholder)
        editedrowsholder={}
        }
  
  
      });
      console.log("First iteration:", finaleditedholder)
     //Extracts the unique ID and dataset names from array which contain edits and store in temp arrays
      for(let j=0; j< finaleditedholder.length; j++){
        tempIDnameholder.push(finaleditedholder[j]["ID"])
        tempdatasetnameholder.push(finaleditedholder[j]["Dataset_Name"])
      }
      
      editedrowsholder= {}
      //iterate thru every row in table, check if row cell values(dataset name and ID) exist in temp arrays or not
      //If condition (dataset exist, field name does not exist, came from editable schema ===true) is fuifilled, 
      //Takes the row and insert above the row containing the same dataset name in finaleditedholder
      example.rows().every(function(){
        if((tempdatasetnameholder.includes(this.data()[2]) && !tempIDnameholder.includes(parseInt(this.data()[0])))===true){
          let date = new Date();
          Object.assign(editedrowsholder,({"ID": parseInt(this.data()[0]), "Origin": this.data()[10], "Platform_Name": this.data()[1], "Dataset_Name": this.data()[2],
          "Global_Tags": ($(example.cell(this.index(), 3).node()).find('input').val()), "Field_Name": this.data()[4], 
          "Editable_Tags": ($(example.cell(this.index(), 5).node()).find('input').val()),
          "Original_Tags": ($(example.cell(this.index(), 6).node()).find('input').val()),
          "Description": ($(example.cell(this.index(), 7).node()).find('input').val()), "Date_Modified": Date.parse(date.toLocaleString())}))
          //If row id of row with same dataset name of edited array is > current selected row, insert row from temp array before, else insert after
          if(finaleditedholder[tempdatasetnameholder.indexOf(this.data()[2])]["ID"] > this.data()[0]){
            insertAt(finaleditedholder, tempdatasetnameholder.indexOf(this.data()[2]), editedrowsholder)
          }else{
            insertAt(finaleditedholder, tempdatasetnameholder.indexOf(this.data()[2]) +1, editedrowsholder)
          }
          
          editedrowsholder={}
          }
    });

      tempIDnameholder=[]
      tempdatasetnameholder=[]
      console.log("Second iteration:", finaleditedholder) 
      

    

    
    axios.post('http://localhost:8000/getresult',
  
    
    finaleditedholder
    
  ,{
        headers: {
          // Overwrite Axios's automatically set Content-Type
          'Content-Type': 'application/json'
        }
      }
    )
    .then(res =>  
      
      
    {
      
      
      window.alert(res.data)
      window.location.reload();
      
    
  })
  .catch(error => {
    window.alert("Error, Try refresh first and try again\r\n\r\nIf not " +error.response.data)
    window.location.reload(); //Logs a string: Error: Request failed with status code 404
  
  });
     
    
  

   
    
  });

  
// this number is the timeout timer setting, IMPORTANT IF UR RECORDS TAKE LONGER, SET A LONGER TIMEOUT
}, 250);
  
  
 }

 
  render(){
    //Datatable HTML
  return (
    <div className="MainDiv">
      <div class="jumbotron text-center">
          <h3>Datahub Tagging UI</h3>
      </div>
      
      <div className="container" >
          
      <table id="example" class="table table-striped table-bordered table-sm row-border hover mb-5">
          <thead>
            <tr>
            {this.state.cols.map((result) => {
            return (
              <th>{result}</th>
          )
          })}
              
              
            </tr>
          </thead>
          <tbody>
          {this.state.rows.map((result) => {
            return (
              <tr class="table-success">
     
                  <td>{result.ID}</td>
                  <td>{result.Platform_Name}</td>
                  <td>{result.Dataset_Name}</td>
                  <td>{result.Global_Tags}</td>
                  <td>{result.Field_Name}</td>
                  <td>{result.Editable_Tags}</td>
                  <td>{result.Original_Tags}</td>
                  <td>{result.Description}</td>
                  <td>{result.Date_Modified}</td>
                  <td>{result.From_EditableSchema}</td>
                  <td>{result.Origin}</td>
                </tr>
          )
          })}
            
          
          </tbody>
        </table>
         
        </div>
      </div>
  );
}
}


export default App;