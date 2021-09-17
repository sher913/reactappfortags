import React from 'react';
import './App.css';
//Bootstrap and jQuery libraries
import 'bootstrap/dist/css/bootstrap.min.css';
import 'jquery/dist/jquery.min.js';
//Datatable Modules
import "datatables.net-dt/js/dataTables.dataTables";
import "datatables.net-dt/css/jquery.dataTables.min.css";
import $, { isEmptyObject } from 'jquery'; 
//For API Requests
import axios from 'axios';
//For tab panes
import Tabs from 'react-bootstrap/Tabs'
import Tab from 'react-bootstrap/Tab'
import TabContainer from 'react-bootstrap/TabContainer'
import TabContent from 'react-bootstrap/TabContent'
import TabPane from 'react-bootstrap/TabPane'
class App extends React.Component {
  
  
  //Declare data store variables
  constructor(props) {
    super(props)
      this.state = {
        rows: [],
        fieldcols:[],
        datasetcols:[],
        datasetrows: []
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
    //array holders for rows(rowsholder) and 2 column name holder(fieldcolsholder and datasetcolsholder)
    const fieldcolsholder =[]

    const datasetcolsholder=[]
  
    const finalrowsholder=[]


    var finaleditedholder=[]

    var elements
    var BrowsePathsholder=[]



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
        
        //If have editableschemametadata but fieldpaths dont match, set to NO
      }else{
        let tagsholder= []
        tagsholder.push(' ')
        Object.assign(rowsholder,({"Editable_Tags": tagsholder}))
        
      } 
      //If do not have editableschemametadata at all
    }else{
      let tagsholder= []
      tagsholder.push(' ')
      Object.assign(rowsholder,({"Editable_Tags": tagsholder}))
     
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
      //for dataset Browsepaths
      if(elements[i]["BrowsePaths"] !== undefined){
        let BrowsePathsholder=[]
   
        if(elements[i]["BrowsePaths"]["paths"]!==[]){
          for(let j=0; j< elements[i]["BrowsePaths"]["paths"].length; j++){
            if(j>0){
            BrowsePathsholder.push(', ',elements[i]["BrowsePaths"]["paths"][j])
          }else{
            BrowsePathsholder.push(elements[i]["BrowsePaths"]["paths"][j])
          }}
        
        }else{
          BrowsePathsholder=[]
          BrowsePathsholder.push(' ')
        }
        Object.assign(rowsholder,({ "Dataset_BrowsePath": BrowsePathsholder}))
      }else{
        BrowsePathsholder=[]
        BrowsePathsholder.push(' ')
        Object.assign(rowsholder,({ "Dataset_BrowsePath": BrowsePathsholder}))
      }

      //for dataset description, the if conditions are terrible but required
      if(elements[i]["editableDatasetProperties"]!==undefined){
        if(elements[i]["editableDatasetProperties"]["description"]!==undefined){
          Object.assign(rowsholder,({ "Dataset_Description": elements[i]["editableDatasetProperties"]["description"]}))
        }else{
          if(elements[i]["DatasetProperties"]!== undefined){
            if(elements[i]["DatasetProperties"]["description"]!== undefined){
              Object.assign(rowsholder,({ "Dataset_Description": elements[i]["DatasetProperties"]["description"]}))
            }else{
              Object.assign(rowsholder,({ "Dataset_Description": ' '}))
            }
          }else{
            Object.assign(rowsholder,({ "Dataset_Description": ' '}))
          }
        }
      }else if(elements[i]["DatasetProperties"]!== undefined){
        if(elements[i]["DatasetProperties"]["description"]!== undefined){
          Object.assign(rowsholder,({ "Dataset_Description": elements[i]["DatasetProperties"]["description"]}))
        }else{
          Object.assign(rowsholder,({ "Dataset_Description": ' '}))
        }
      }else{
        Object.assign(rowsholder,({ "Dataset_Description": ' '}))
      }
      
      
      finalrowsholder.push(rowsholder)
      rowsholder = {}
      
    }
  
  }
  //Columns header defintion #important
    fieldcolsholder.push("#", "Platform_Name", "Dataset_Name", "Field_Name", "Editable_Tags","Original_Tags", "Description", "Date_Modified")
    datasetcolsholder.push("Platform_Name", "Dataset_Name", "Dataset_BrowsePath", "Global_Tags","Dataset_Description", "Date_Modified","Origin")
   
    const datasetrowsholder= []
    var tempdatasetrowNames=[]

    for(let j=0; j< finalrowsholder.length; j++){
      if (!(tempdatasetrowNames.includes(finalrowsholder[j].Dataset_Name))){
        datasetrowsholder.push(finalrowsholder[j])
        tempdatasetrowNames.push(finalrowsholder[j].Dataset_Name)
      }
    }
  
    console.log("Sorted fields of data retrived from GMS:",elements)
    console.log("Column Headers:",fieldcolsholder)
    
    console.log("Data to feed dataset columns:",datasetrowsholder)
    console.log("Data to feed field columns:",finalrowsholder)


    this.setState({datasetrows: datasetrowsholder,rows: finalrowsholder, fieldcols: fieldcolsholder, datasetcols: datasetcolsholder});
       }); 
   
    //init Datatable, #example is the table element id
    setTimeout(()=>{                        
      var example =$('#example').DataTable(
        {order: [[ 0, "asc" ]],
          responsive: true,
       
       
          "lengthMenu": [[10, 20, 100, -1], [10, 20, 100, "All"]],
          columnDefs : [
            { "type": "html-input", targets: [4,5,6],
              render: function (rows, type, row) {
            
                return '<input class="form-control" type="text"  value ="'+ rows + '" style= "width:auto">';
              
              }
              
            }, {
              "targets": [0],
              "visible": false,
              "searchable": false
          }
          ]
          
        }
  
      )
      var example2 =$('#example2').DataTable(
        {order: [[ 0, "asc" ]],
          responsive: true,
       
       
          "lengthMenu": [[10, 20, 100, -1], [10, 20, 100, "All"]],
          columnDefs : [
            { "type": "html-input", targets: [2,3,4],
              render: function (rows, type, row) {
            
                return '<input class="form-control" type="text"  value ="'+ rows + '" style= "width:auto">';
              
              }
              
            }, {
              "targets": [6],
              "visible": false,
              "searchable": false
          }
          ]
          
        }
  
      )

 //Iterate thru all row and compare original data vs edited, if edited, add to array (finaleditedholder) to be sent to endpoint
  $('#test').click(function () {
    let editedrowsholder = {};
    let tempIDnameholder=[];
    let tempdatasetnameholder=[];
    let finaleditedholder=[];

    function anyChangesfromFields() {example.rows().every(function(){
      if(this.data()[4] !== ($(example.cell(this.index(), 4).node()).find('input').val()) 
      ||this.data()[5] !== ($(example.cell(this.index(), 5).node()).find('input').val())
      ||this.data()[6] !== ($(example.cell(this.index(), 6).node()).find('input').val())
      ){
        //Extracts the edited dataset names from array which contain edits and store in temp arrays
        tempdatasetnameholder.push(this.data()[2])
      }})};
    function anyChangesfromDatasets() {example2.rows().every(function(){
      if(this.data()[2] !== ($(example2.cell(this.index(), 2).node()).find('input').val())
      ||this.data()[3] !== ($(example2.cell(this.index(), 3).node()).find('input').val())
      ||this.data()[4] !== ($(example2.cell(this.index(), 4).node()).find('input').val())
      ){
        if(!tempdatasetnameholder.includes(this.data()[1])){
        tempdatasetnameholder.push(this.data()[1])
        }
      }
    })}

      //iterate thru every row in table, check if row cell values(dataset name and ID) exist in temp arrays or not
      //If condition (dataset exist, Unique ID does not exist) is fuifilled, 
      //Takes the row and insert in finaleditedholder
      function addAllFieldsfromDataset() {example.rows().every(function(){
        if((tempdatasetnameholder.includes(this.data()[2]) && !tempIDnameholder.includes(parseInt(this.data()[0])))===true){
          let date = new Date();
          Object.assign(editedrowsholder,({"ID": parseInt(this.data()[0]), "Platform_Name": this.data()[1], "Dataset_Name": this.data()[2],
          "Field_Name": this.data()[3], 
          "Editable_Tags": ($(example.cell(this.index(), 4).node()).find('input').val()),
          "Original_Tags": ($(example.cell(this.index(), 5).node()).find('input').val()),
          "Description": ($(example.cell(this.index(), 6).node()).find('input').val()), 
          "Date_Modified": Date.parse(date.toLocaleString()),
       
        }))
          //If row id of row with same dataset name of edited array is > current selected row, insert row from temp array before, else insert after
          finaleditedholder.push(editedrowsholder)
          
          editedrowsholder={}
          }
    })};
      //Adds the dataset level properties to each field objects
      function addDatasetProperties() {example2.rows().every(function(){
        for(let j=0; j< finaleditedholder.length; j++){
          if((this.data()[0])=== finaleditedholder[j].Platform_Name && (this.data()[1])=== finaleditedholder[j].Dataset_Name){
            Object.assign(finaleditedholder[j],({
              "Browse_Path": ($(example2.cell(this.index(), 2).node()).find('input').val()),
              "Global_Tags": ($(example2.cell(this.index(), 3).node()).find('input').val()),
              // finaleditedholder[j]["Dataset_Description"] = ($(example2.cell(this.index(), 4).node()).find('input').val()),
              "Origin": this.data()[6]
      
            }))
            
          
          }
         
        }
      }
      
      )};

    //Checks for changes in fields table, add dataset to tempdatasetArray
    anyChangesfromFields();
    //Checks for changes in dataset table, add dataset to tempdatasetArray if not in Array
    anyChangesfromDatasets();
    // Assigns all fields with changes or not to an object base on Tempdatasetholder and TempIdholder
    addAllFieldsfromDataset();
    //Adds dataset level properties to the fields assigned to the object
    addDatasetProperties();
    console.log("Payload to send to FASTAPI: ",finaleditedholder)
    

 

    
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
}, 550);
  
  
 }

 
  render(){
    //Datatable HTML
  return (
    <div className="MainDiv">
      <div class="jumbotron text-center">
          <h3>Datahub Tagging UI</h3>
      </div>
  <div className="container" >
  <Tabs fill defaultActiveKey="Datasets"  id="uncontrolled-tab-example" className="mb-3">
    <Tab eventKey="Datasets" title="Datasets">
    
    
          
    <table id="example2" class="table table-striped table-bordered table-sm row-border hover mb-5"> 
        <thead>
          <tr>
          {this.state.datasetcols.map((result) => {
          return (
            <th>{result}</th>
        )
        })}
            
            
          </tr>
        </thead>
        <tbody>
        {this.state.datasetrows.map((result) => {
          return (
            <tr class="table-success">
   
                <td>{result.Platform_Name}</td>
                <td>{result.Dataset_Name}</td>
                <td>{result.Dataset_BrowsePath}</td>
                <td>{result.Global_Tags}</td>
                <td>{result.Dataset_Description}</td>
                <td>{result.Date_Modified}</td>
                <td>{result.Origin}</td>
                
              </tr>
        )
        })}
          
        
        </tbody>
      </table>
    
    </Tab>
    <Tab eventKey="Fields" title="Fields">
  
 
          
    <table id="example" class="table table-striped table-bordered table-sm row-border hover mb-5"> 
        <thead>
          <tr>
          {this.state.fieldcols.map((result) => {
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
                <td>{result.Field_Name}</td>
                <td>{result.Editable_Tags}</td>
                <td>{result.Original_Tags}</td>
                <td>{result.Description}</td>
                <td>{result.Date_Modified}</td>
              </tr>
        )
        })}
          
        
        </tbody>
      </table>
       
    
    </Tab>
   
    
  </Tabs>
  </div>
      </div>
  );
}
}


export default App;