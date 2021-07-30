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


    
    // on here, nid to make Python FASTAPI as middleware to bypass CORS, then axios.get(http://localhost/FASTAPI)
    axios.get('http://localhost:8000/getdatasets', {
      headers: {
          'X-RestLi-Protocol-Version': '2.0.0',
          'X-RestLi-Method': 'finder'      
      }
    }).then(res => 
    { //pushing datasets data to 'elements' varaiable
      let elements = (res["data"]["elements"])
      console.log(typeof elements[0]["schemaMetadata"]["lastModified"]["time"])
      let count =0
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
        Object.assign(rowsholder, {"Platform_Name": (elements[i]["platform"]).split(':').pop()});
        Object.assign(rowsholder,{"Table_Name": elements[i]["name"]});
      
        //For elements with global tags, if they not equal to undefined, push the tags to array, else push ' ' to array
        if(elements[i]["globalTags"]!==undefined){
          let globaltagholder= []
          
        
          for(let k=0; k< elements[i]["globalTags"]["tags"].length; k++){
            if(k>0){
              globaltagholder.push(', '+ elements[i]["globalTags"]["tags"][k]["tag"].split(':').pop())
            }
            else{
        globaltagholder.push(elements[i]["globalTags"]["tags"][k]["tag"].split(':').pop())
    }
  }
    Object.assign(rowsholder, ({"Global_Tags": globaltagholder}))
 
  }     else{
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
        
        //This line matches [x] element in array of editableschema to [x] element in array of schemadata, but wont workk because the element index of editable does not match schemametadata element index
        && elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["fieldPath"] === elements[i]["schemaMetadata"]["fields"][j]["fieldPath"])
     
        {
          let tagsholder= []
          for(let l=0; l< elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"].length; l++){
            if(l>0){
              tagsholder.push(', ' + (elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"][l]["tag"].split(':').pop()))
            }else{
            tagsholder.push((elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"][l]["tag"].split(':').pop()))
        }
      }
        Object.assign(rowsholder, ({"Tags_For_Field": tagsholder}))
      }
    }
       
      //Use schemadata tag if exist, since no editableSchemaMetaData
      if(elements[i]["schemaMetadata"]["fields"][j]["globalTags"]!==undefined){
        
        let tagsholder= []
          for(let m=0; m< elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"].length; m++){
            if(m>0){
          tagsholder.push(', ' + (elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"][m]["tag"].split(':').pop()))
         
        }else{
          tagsholder.push((elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"][m]["tag"].split(':').pop()))
      }
    }
        Object.assign(rowsholder,({"Tags_For_Field": tagsholder}))
      }

      //If both don't exist, push a blank
      if (elements[i]["editableSchemaMetadata"] === undefined && elements[i]["schemaMetadata"]["fields"][j]["globalTags"] === undefined){
        let tagsholder= []
        tagsholder.push(' ')
        Object.assign(rowsholder,({"Tags_For_Field": tagsholder}))
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
    colsholder.push("#", "Platform_Name", "Table_Name","Global_Tags", "Field_Name", "Tags_For_Field", "Description", "Date_Modified")
   
      // testing
      //console.log(elements)
      // console.log("Platform name:", (elements[0]["platform"]).split(':').pop())
      // console.log("table name:", elements[0]["name"])
      // console.log("Global Tags:", elements[0]["globalTags"]["tags"])
      // console.log("Field name:", elements[0]["schemaMetadata"]["fields"][0]["fieldPath"])
      // console.log("Tag name for field:", (elements[0]["editableSchemaMetadata"]["editableSchemaFieldInfo"][0]["globalTags"]["tags"][0]["tag"].split(':').pop()))
      // console.log("Description:", elements[0]["schemaMetadata"]["fields"][0]["description"])
      // if((elements[0]["editableSchemaMetadata"])=== undefined || (elements[0]["editableSchemaMetadata"]) ==0)
      // {
      //   console.log("Last Modified:", Date(elements[0]["schemaMetadata"]["lastModified"]["time"]).toLocaleString())
      // } else{
      //   console.log("Last Modified:", Date(elements[0]["editableSchemaMetadata"]["lastModified"]["time"]).toLocaleString())
      // }
      
      
      
      //Storing users detail in state array object
      //for(let i = 0; i < res.data.response[0].length; i++){
      // colsholder.push(res.data.response[0][i]['Field']) 
      //}
    
      //for(let i = 0; i < res.data.response[1].length; i++){
     // rowsholder =(res.data.response[1][i]) 
     //}
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
     
        
        "lengthMenu": [[5, 10, 15, -1], [5, 10, 15, "All"]],
        columnDefs : [
          { "type": "html-input", targets: [3,5,6],
            render: function (rows, type, row) {
          
              return '<input class="form-control" type="text"  value ="'+ rows + '" style= "width:auto">';
            
            }
            
          }
        ]
        
      }

    )

 
 //Iterate thru all row and compare original data vs edited, if edited, add to array (finaleditedholder) to be sent to endpoint
  $('#test').click(function () {
    let editedrowsholder = {};
    finaleditedholder=[]
    example.rows().every(function(){
    
      if(this.data()[3] !== ($(example.cell(this.index(), 3).node()).find('input').val()) 
      ||this.data()[5] !== ($(example.cell(this.index(), 5).node()).find('input').val())
      ||this.data()[6] !== ($(example.cell(this.index(), 6).node()).find('input').val())
      ){
        let date = new Date();
        Object.assign(editedrowsholder,({"ID": parseInt(this.data()[0]), "Platform_Name": this.data()[1], "Table_Name": this.data()[2],
        "Global_Tags": ($(example.cell(this.index(), 3).node()).find('input').val()), "Field_Name": this.data()[4], 
        "Tags_For_Field": ($(example.cell(this.index(), 5).node()).find('input').val()),
        "Description": ($(example.cell(this.index(), 6).node()).find('input').val()), "Date_Modified": Date.parse(date.toLocaleString())}))
        finaleditedholder.push(editedrowsholder)
        editedrowsholder={}
       
      }
  
  
      });
      console.log("submitted:", finaleditedholder)


    
    axios.post('http://localhost:8000/getresult',
    finaleditedholder
  
  ,{
        headers: {
          // Overwrite Axios's automatically set Content-Type
          'Content-Type': 'application/json'
        }
      }
    )
    .then(res =>  console.log("Response from what API received: ", res.data))
   
    
  });

  

  }, 100);
  
  
  
  


 

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
                  <td>{result.Table_Name}</td>
                  <td>{result.Global_Tags}</td>
                  <td>{result.Field_Name}</td>
                  <td>{result.Tags_For_Field}</td>
                  <td>{result.Description}</td>
                  <td>{result.Date_Modified}</td>
            
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