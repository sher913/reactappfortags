import React from 'react';
import './App.css';
//Bootstrap and jQuery libraries
import 'bootstrap/dist/css/bootstrap.min.css';
import 'jquery/dist/jquery.min.js';
//Datatable Modules
import "datatables.net-dt/js/dataTables.dataTables";
import "datatables.net-dt/css/jquery.dataTables.min.css";
import $, { data } from 'jquery'; 
//For API Requests
import axios from 'axios';
class App extends React.Component {
  //Declare data store variables
  constructor(props) {
    super(props)
      this.state = {
        data: [],
        cols:[]
              }
      }

 
  componentDidMount() {
    //Get all users details and table columns names in bootstrap table
    const colsholder =[]
    const rowsholder=[]
    
    // on here, nid to make Python FASTAPI as middleware to bypass CORS, then axios.get(http://localhost/FASTAPI)
    axios.get('http://localhost:8000/getdatasets', {
      headers: {
          'X-RestLi-Protocol-Version': '2.0.0',
          'X-RestLi-Method': 'finder'
        
      
          
      
      }
    }).then(res => 
    { //pushing datasets data to 'elements' varaiable
      let elements = (res["data"]["elements"])
    //for loop for total datasets iteration
    for(let i=0; i< elements.length; i++){
      for(let j=0; j< elements[i]["schemaMetadata"]["fields"].length; j++){
      
      //for loop for platform and table name of datasets
      console.log("platform name: ",(elements[i]["platform"]).split(':').pop());
      console.log( elements[i]["name"]);
      //For elements with global tags, if they not equal to undefined, push the tags to array, else push ' ' to array
      if(elements[i]["globalTags"]!==undefined){
        let globaltagholder= []
        
      
        for(let k=0; k< elements[i]["globalTags"]["tags"].length; k++){
          
       
      globaltagholder.push(elements[i]["globalTags"]["tags"][k]["tag"].split(':').pop())
    }
    console.log("global  tags: ", globaltagholder)
 
  }     else{
          let globaltagholder= []
          globaltagholder.push(' ')
          console.log("global  tags: ", globaltagholder)
  }
   
    //injest field name
      console.log(elements[i]["schemaMetadata"]["fields"][j]["fieldPath"])
      //if the dataset even has editableSchemadata
      if(elements[i]["editableSchemaMetadata"]!==undefined){
        //Field in editableSchemaMetadata has to match fields in schemaMetadata
        if(elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]!==undefined && elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["fieldPath"]===elements[i]["schemaMetadata"]["fields"][j]["fieldPath"]){
          let tagsholder= []
          for(let l=0; l< elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"].length; l++){
            tagsholder.push((elements[i]["editableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"][l]["tag"].split(':').pop()))
        }
        console.log("tags name:",tagsholder)
      } 
      } 
      //Use schemadata tag if exist, since no editableSchemaMetaData
      if(elements[i]["schemaMetadata"]["fields"][j]["globalTags"]!==undefined){
        
        let tagsholder= []
          for(let m=0; m< elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"].length; m++){
          tagsholder.push((elements[i]["schemaMetadata"]["fields"][j]["globalTags"]["tags"][m]["tag"].split(':').pop()))
         
        }
        console.log("tags name:",tagsholder)
        //If both don't exist, push a blank
      } else{
        let tagsholder= []
        tagsholder.push(' ')
        console.log("tags name:",tagsholder)
      }
      console.log("Description:", elements[0]["schemaMetadata"]["fields"][0]["description"])


    }}
      

      // testing
      console.log("Elements:", elements)
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
     // rowsholder.push(res.data.response[1][i]) 
     //}
    //console.log(colsholder)
   //console.log(rowsholder)
    //this.setState({data: rowsholder, cols: colsholder});
       }); 
    //init Datatable  
    setTimeout(()=>{                        
    $('#example').DataTable(
      {
        "lengthMenu": [[5, 10, 15, -1], [5, 10, 15, "All"]]
      }
    );
  }, 100);
 }

 
  render(){
    //Datatable HTML
  return (
    <div className="MainDiv">
      <div class="jumbotron text-center">
          <h3>Sher's Intern Journey</h3>
      </div>
      
      <div className="container">
          
      <table id="example" class="table table-striped table-bordered table-sm row-border hover mb-5" >
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
          {this.state.data.map((result) => {
            return (
              <tr class="table-success">
                  
                  
                  <td>{result.urn}</td>
                  <td>{result.aspect}</td>
                  <td>{result.version}</td>
                  <td>{result.metadata}</td>
                  <td>{result.systemmetadata}</td>
                  <td>{result.createdon}</td>
                  <td>{result.createdby}</td>
                  <td>{result.createdfor}</td>
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