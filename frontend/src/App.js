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
    axios.get('/metadata').then(res => 
    {
      //Storing users detail in state array object
      for(let i = 0; i < res.data.response[0].length; i++){
       colsholder.push(res.data.response[0][i]['Field']) 
      }
    
      for(let i = 0; i < res.data.response[1].length; i++){
      rowsholder.push(res.data.response[1][i]) 
     }
    console.log(colsholder)
    console.log(rowsholder)
    this.setState({data: rowsholder, cols: colsholder});
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