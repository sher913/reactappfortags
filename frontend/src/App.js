import React from "react";
import "./App.css";
//Bootstrap and jQuery libraries
import "bootstrap/dist/css/bootstrap.min.css";
import "jquery/dist/jquery.min.js";
//Datatable Modules
import "datatables.net-dt/js/dataTables.dataTables";
import "datatables.net-dt/css/jquery.dataTables.min.css";
import $ from "jquery";
//For API Requests
import axios from "axios";
//For tab panes
import Tabs from "react-bootstrap/Tabs";
import Tab from "react-bootstrap/Tab";

class App extends React.Component {
  //Declare data store variables
  constructor(props) {
    super(props);
    this.state = {
      fieldrows: [],
      fieldcols: [],
      datasetrows: [],
      datasetcols: [],
      tagrows: [],
      tagscols: [],
    };
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
    }
    const convertArrayToObject = (array, key) => {
      const initialValue = {};
      return array.reduce((obj, item) => {
        return {
          ...obj,
          [item[key]]: item,
        };
      }, initialValue);
    };

    //array holders for fieldrows(rowsholder) and 2 column name holder(fieldcolsholder and datasetcolsholder)
    const fieldcolsholder = [];

    const datasetcolsholder = [];

    const tagscolsholder = [];

    const finalrowsholder = [];
    console.log("Timeout setting:", process.env.REACT_APP_TIMEOUT_SETTING ?? "is undefined, so using default value of 3000", "ms");
    //Change this endpoint depending on ur datahub endpoint, uses http://localhost:9002 if not defined
    const datahub_address = process.env.REACT_APP_DATAHUB_ADDRESS ?? "http://locahost:9002/";
    var elements;
    var BrowsePathsholder = [];
    var allTagsObject;

    // on here, nid to make Python FASTAPI as middleware to bypass CORS, then axios.get(http://localhost/FASTAPI)
    axios.get("http://localhost:8000/getdatasets", {}).then((res) => {
      //pushing datasets data to 'elements' varaiable
      console.log("Datasets dopped: ", res["data"][1]);
      elements = res["data"][0];
      allTagsObject = res["data"][2];
      let count = 0;
      // aspectSchemaMetadata=['aspects']+['com.linkedin.schema.SchemaMetadata']

      //For loop for all fields in dataset, compare with editableSchema fields; if exist, push both to first element of each array, thus index positions of both edited Schema
      // and Schemameta(original) will match
      for (let i = 0; i < elements.length; i++) {
        for (let j = 0; j < elements[i]["SchemaMetadata"]["fields"].length; j++) {
          if (elements[i]["EditableSchemaMetadata"] !== undefined) {
            for (let a = 0; a < elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"].length; a++) {
              if (elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][a]["fieldPath"] === elements[i]["SchemaMetadata"]["fields"][j]["fieldPath"]) {
                moveArrayItemToNewIndex(elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"], a, 0);
                moveArrayItemToNewIndex(elements[i]["SchemaMetadata"]["fields"], j, 0);
              }
            }
          }
        }
      }

      //for loop for total datasets iteration
      for (let i = 0; i < elements.length; i++) {
        //To capture Tag Count of GlobalTags which is dataset level hence, placed above fields for loop
        let distinctTagCountChecker = new Set();
        if (elements[i]["GlobalTags"] !== undefined) {
          let datasetGlobalTags = elements[i]["GlobalTags"]["tags"].map((tags) => tags["tag"].split("urn:li:tag:").pop());
          for (let k = 0; k < datasetGlobalTags.length; k++) {
            if (datasetGlobalTags[k] in allTagsObject) {
              //Adds to the count counter by one for each tag captured accordingly
              allTagsObject[datasetGlobalTags[k]]["Count"] += 1;
            }
            //Mostly redundant since we gather all tags in database, however it could capture anomalies of tags that did not end up in database but assigned to dataset
            else {
              allTagsObject[datasetGlobalTags[k]] = {
                Tag: datasetGlobalTags[k],
                Count: 1,
              };
            }
            distinctTagCountChecker.add(datasetGlobalTags[k]);
          }
        }
        for (let j = 0; j < elements[i]["SchemaMetadata"]["fields"].length; j++) {
          let rowsholder = {};

          Object.assign(rowsholder, { ID: count });
          count += 1;
          //for loop for platform and table name of datasets, always add key and value pair when pushing to array so aDataSort can refrence later
          Object.assign(rowsholder, {
            Origin: elements[i]["DatasetKey"]["origin"],
          });
          Object.assign(rowsholder, {
            Platform_Name: elements[i]["DatasetKey"]["platform"].split("urn:li:dataPlatform:").pop(),
          });
          Object.assign(rowsholder, {
            Dataset_Name: elements[i]["DatasetKey"]["name"],
          });

          //For elements with global tags, if they not equal to undefined, push the tags to array, else push ' ' to array
          if (elements[i]["GlobalTags"] !== undefined) {
            let globaltagholder = [];

            if (elements[i]["GlobalTags"]["tags"].length === 0) {
              globaltagholder.push(" ");
            } else {
              globaltagholder.push(elements[i]["GlobalTags"]["tags"].map((tags) => tags["tag"].split("urn:li:tag:").pop()).join(", "));
            }

            Object.assign(rowsholder, { Global_Tags: globaltagholder });
          } else {
            let globaltagholder = [];
            globaltagholder.push(" ");
            Object.assign(rowsholder, { Global_Tags: globaltagholder });
          }

          //injest field name
          Object.assign(rowsholder, {
            Field_Name: elements[i]["SchemaMetadata"]["fields"][j]["fieldPath"],
          });

          //if the dataset even has editableSchemadata
          if (elements[i]["EditableSchemaMetadata"] !== undefined) {
            //Field in EditableSchemaMetadata has to match fields in SchemaMetadata
            if (
              elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j] !== undefined &&
              elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j]["fieldPath"] === elements[i]["SchemaMetadata"]["fields"][j]["fieldPath"]
            ) {
              let tagsholder = [];
              if (elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"] !== undefined) {
                if (elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"].length === 0) {
                  tagsholder.push(" ");
                } else {
                  let editableFieldTags = elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j]["globalTags"]["tags"].map((tags) =>
                    tags["tag"].split("urn:li:tag:").pop()
                  );
                  tagsholder.push(editableFieldTags.join(", "));
                  for (let l = 0; l < editableFieldTags.length; l++) {
                    //Check if tag has been captured for the dataset, else captures Tag count for editableSchema field Tags.
                    if (!distinctTagCountChecker.has(editableFieldTags[l])) {
                      if (editableFieldTags[l] in allTagsObject) {
                        allTagsObject[editableFieldTags[l]]["Count"] += 1;
                      }
                      //Mostly redundant, however it could capture anomalies of tags that did not end up in database but assigned to dataset
                      else {
                        allTagsObject[editableFieldTags[l]] = {
                          Tag: editableFieldTags[l],
                          Count: 1,
                        };
                      }
                      distinctTagCountChecker.add(editableFieldTags[l]);
                    }
                  }
                }
              }
              Object.assign(rowsholder, { Editable_Tags: tagsholder });

              //If have editableschemametadata but fieldpaths dont match, capture empty string instead
            } else {
              let tagsholder = [];
              tagsholder.push(" ");
              Object.assign(rowsholder, { Editable_Tags: tagsholder });
            }
            //If do not have editableschemametadata at all
          } else {
            let tagsholder = [];
            tagsholder.push(" ");
            Object.assign(rowsholder, { Editable_Tags: tagsholder });
          }

          //Filling tags from schemametadata
          if (elements[i]["SchemaMetadata"]["fields"][j]["globalTags"] !== undefined) {
            let tagsholder = [];
            if (elements[i]["SchemaMetadata"]["fields"][j]["globalTags"]["tags"].length === 0) {
              tagsholder.push(" ");
            } else {
              let schemaFieldTags = elements[i]["SchemaMetadata"]["fields"][j]["globalTags"]["tags"].map((tags) => tags["tag"].split("urn:li:tag:").pop());
              tagsholder.push(schemaFieldTags.join(", "));
              for (let m = 0; m < schemaFieldTags.length; m++) {
                //Check if tag has been captured for the dataset, else captures Tag Count of Schemametdata field Tags

                if (!distinctTagCountChecker.has(schemaFieldTags[m])) {
                  if (schemaFieldTags[m] in allTagsObject) {
                    allTagsObject[schemaFieldTags[m]]["Count"] += 1;
                  }
                  //Mostly redundant, however it could capture anomalies of tags that did not end up in database but assigned to dataset
                  else {
                    allTagsObject[schemaFieldTags[m]] = {
                      Tag: schemaFieldTags[m],
                      Count: 1,
                    };
                  }
                }
                distinctTagCountChecker.add(schemaFieldTags[m]);
              }
            }
            Object.assign(rowsholder, { Original_Tags: tagsholder });
          } else {
            let tagsholder = [];
            tagsholder.push(" ");
            Object.assign(rowsholder, { Original_Tags: tagsholder });
          }

          //Checks for Description in editableschemaMetaData first, then checks in SchemaMetaData.
          if (elements[i]["EditableSchemaMetadata"] !== undefined) {
            if (
              elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j] !== undefined &&
              elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j]["fieldPath"] === elements[i]["SchemaMetadata"]["fields"][j]["fieldPath"] &&
              elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j]["description"] !== undefined
            ) {
              Object.assign(rowsholder, {
                Description: elements[i]["EditableSchemaMetadata"]["editableSchemaFieldInfo"][j]["description"],
              });
            } else if (elements[i]["SchemaMetadata"]["fields"][j]["description"] !== undefined) {
              Object.assign(rowsholder, {
                Description: elements[i]["SchemaMetadata"]["fields"][j]["description"],
              });
            }
          }

          //Since already checked in editableschemaMetaData , now just checks in schemametadata, else if empty, fill with blank
          if (elements[i]["EditableSchemaMetadata"] === undefined && elements[i]["SchemaMetadata"]["fields"][j]["description"] !== undefined) {
            Object.assign(rowsholder, {
              Description: elements[i]["SchemaMetadata"]["fields"][j]["description"],
            });
          }
          if (elements[i]["EditableSchemaMetadata"] === undefined && elements[i]["SchemaMetadata"]["fields"][j]["description"] === undefined) {
            Object.assign(rowsholder, { Description: " " });
          }
          //for Timestamp, checks if editableschemametadata exists, if not use schemametadata
          if (elements[i]["EditableSchemaMetadata"] === undefined) {
            let date = new Date(elements[i]["SchemaMetadata"]["lastModified"]["time"]);

            Object.assign(rowsholder, {
              Date_Modified: date.toLocaleString(),
            });
          } else {
            let date = new Date(elements[i]["EditableSchemaMetadata"]["lastModified"]["time"]);
            Object.assign(rowsholder, {
              Date_Modified: date.toLocaleString(),
            });
          }
          //for dataset Browsepaths
          if (elements[i]["BrowsePaths"] !== undefined) {
            let BrowsePathsholder = [];

            if (elements[i]["BrowsePaths"]["paths"] !== []) {
              for (let j = 0; j < elements[i]["BrowsePaths"]["paths"].length; j++) {
                if (j > 0) {
                  BrowsePathsholder.push(", ", elements[i]["BrowsePaths"]["paths"][j]);
                } else {
                  BrowsePathsholder.push(elements[i]["BrowsePaths"]["paths"][j]);
                }
              }
            } else {
              BrowsePathsholder = [];
              BrowsePathsholder.push(" ");
            }
            Object.assign(rowsholder, {
              Dataset_BrowsePath: BrowsePathsholder,
            });
          } else {
            BrowsePathsholder = [];
            BrowsePathsholder.push(" ");
            Object.assign(rowsholder, {
              Dataset_BrowsePath: BrowsePathsholder,
            });
          }

          //for dataset description, the if conditions are terrible but required
          if (elements[i]["EditableDatasetProperties"] !== undefined) {
            if (elements[i]["EditableDatasetProperties"]["description"] !== undefined) {
              Object.assign(rowsholder, {
                Dataset_Description: elements[i]["EditableDatasetProperties"]["description"],
              });
            } else {
              if (elements[i]["DatasetProperties"] !== undefined) {
                if (elements[i]["DatasetProperties"]["description"] !== undefined) {
                  Object.assign(rowsholder, {
                    Dataset_Description: elements[i]["DatasetProperties"]["description"],
                  });
                } else {
                  Object.assign(rowsholder, { Dataset_Description: " " });
                }
              } else {
                Object.assign(rowsholder, { Dataset_Description: " " });
              }
            }
          } else if (elements[i]["DatasetProperties"] !== undefined) {
            if (elements[i]["DatasetProperties"]["description"] !== undefined) {
              Object.assign(rowsholder, {
                Dataset_Description: elements[i]["DatasetProperties"]["description"],
              });
            } else {
              Object.assign(rowsholder, { Dataset_Description: " " });
            }
          } else {
            Object.assign(rowsholder, { Dataset_Description: " " });
          }

          finalrowsholder.push(rowsholder);
          rowsholder = {};
        }
      }
      //Columns header defintion #important
      fieldcolsholder.push("#", "Platform_Name", "Dataset_Name", "Field_Name", "Editable_Tags", "Original_Tags", "Description", "Date_Modified");
      datasetcolsholder.push("Platform_Name", "Dataset_Name", "Dataset_BrowsePath", "Global_Tags", "Dataset_Description", "Date_Modified", "Origin");
      tagscolsholder.push("Tag", "Description", "Count");

      const datasetrowsholder = [];
      var tempdatasetrowNames = [];

      for (let j = 0; j < finalrowsholder.length; j++) {
        if (!tempdatasetrowNames.includes(finalrowsholder[j].Dataset_Name)) {
          datasetrowsholder.push(finalrowsholder[j]);
          tempdatasetrowNames.push(finalrowsholder[j].Dataset_Name);
        }
      }

      console.log("Sorted fields of data retrived from GMS:", elements);
      console.log("Column Headers:", fieldcolsholder);

      console.log("Data to feed dataset columns:", datasetrowsholder);
      console.log("Data to feed field columns:", finalrowsholder);
      console.log("Data to feed tags columns:", allTagsObject);
      this.setState({
        datasetrows: datasetrowsholder,
        fieldrows: finalrowsholder,
        tagrows: Object.values(allTagsObject),
        fieldcols: fieldcolsholder,
        datasetcols: datasetcolsholder,
        tagscols: tagscolsholder,
      });
    });

    //init Datatable, #fieldTable #datasetTable are the table element ids
    setTimeout(() => {
      var fieldTable = $("#fieldTable").DataTable({
        order: [[0, "asc"]],
        responsive: true,

        lengthMenu: [
          [10, 20, 100, -1],
          [10, 20, 100, "All"],
        ],
        columnDefs: [
          {
            type: "html-input",
            targets: [4, 5, 6],
            render: function (rows, type, row) {
              return '<input class="form-control" type="text"  value ="' + rows + '" style= "width:auto">';
            },
          },
          {
            targets: [0],
            visible: false,
            searchable: false,
          },
        ],
      });
      var datasetTable = $("#datasetTable").DataTable({
        order: [[0, "asc"]],
        responsive: true,

        lengthMenu: [
          [10, 20, 100, -1],
          [10, 20, 100, "All"],
        ],
        columnDefs: [
          {
            type: "html-input",
            targets: [2, 3, 4],
            render: function (rows, type, row) {
              return '<input class="form-control" type="text"  value ="' + rows + '" style= "width:auto">';
            },
          },
          {
            targets: [6],
            visible: false,
            searchable: false,
          },
        ],
      });
      var tagTable = $("#tagTable").DataTable({
        order: [[2, "desc"]],
        responsive: true,

        lengthMenu: [
          [10, 20, 100, -1],
          [10, 20, 100, "All"],
        ],
        columnDefs: [
          {
            type: "html-input",
            targets: [0, 1],
            render: function (rows, type, row) {
              return '<input class="form-control" type="text"  value ="' + rows + '"style= "width:auto">';
            },
          },
        ],
      });

      $("#tagTable").on("click", "td:nth-child(3)", function () {
        window.location.href = datahub_address + `/tag/urn:li:tag:${tagTable.row(this).data()[0]}`;
      });

      //Iterate thru field dataset table and tags table, add edited dataset to a tempArray then use it to add fields' properties and dataset properties to an object and send to Fast API
      $("#test").click(function () {
        let editedrowsholder = {};
        let tempdatasetnameholder = new Set();
        let finaleditedholder = [];
        let changedTagsObjectholder = new Object();

        function anyChangesfromTags() {
          tagTable.rows().every(function () {
            let originalTag = this.data()[0];
            let EditedTag = $(tagTable.cell(this.index(), 0).node()).find("input").val();
            let originalDesc = this.data()[1];
            let EditedDesc = $(tagTable.cell(this.index(), 1).node()).find("input").val();

            if (originalTag !== EditedTag) {
              //To use to change the tag values before submitting to FASTAPI
              changedTagsObjectholder[originalTag] = { Tag: EditedTag, Description: EditedDesc };
              for (let j = 0; j < finalrowsholder.length; j++) {
                let Original_global_tags = finalrowsholder[j].Global_Tags[0].replace(/\s/g, "").split(",");
                let Original_editable_tags = finalrowsholder[j].Editable_Tags[0].replace(/\s/g, "").split(",");
                let Original_original_tags = finalrowsholder[j].Original_Tags[0].replace(/\s/g, "").split(",");
                if (
                  Original_global_tags.some((r) => Object.keys(changedTagsObjectholder).includes(r)) ||
                  Original_editable_tags.some((r) => Object.keys(changedTagsObjectholder).includes(r)) ||
                  Original_original_tags.some((r) => Object.keys(changedTagsObjectholder).includes(r))
                ) {
                  tempdatasetnameholder.add(finalrowsholder[j].Dataset_Name);
                }
              }
            } else if (originalDesc !== EditedDesc) {
              changedTagsObjectholder[originalTag] = { Tag: EditedTag, Description: EditedDesc };
            }
          });
        }

        function anyChangesfromFields() {
          fieldTable.rows().every(function () {
            if (
              this.data()[4] !== $(fieldTable.cell(this.index(), 4).node()).find("input").val() ||
              this.data()[5] !== $(fieldTable.cell(this.index(), 5).node()).find("input").val() ||
              this.data()[6] !== $(fieldTable.cell(this.index(), 6).node()).find("input").val()
            ) {
              //Extracts the edited dataset names from array which contain edits and store in temp arrays
              tempdatasetnameholder.add(this.data()[2]);
            }
          });
        }
        function anyChangesfromDatasets() {
          datasetTable.rows().every(function () {
            if (
              this.data()[2] !== $(datasetTable.cell(this.index(), 2).node()).find("input").val() ||
              this.data()[3] !== $(datasetTable.cell(this.index(), 3).node()).find("input").val() ||
              this.data()[4] !== $(datasetTable.cell(this.index(), 4).node()).find("input").val()
            ) {
              if (!tempdatasetnameholder.has(this.data()[1])) {
                tempdatasetnameholder.add(this.data()[1]);
              }
            }
          });
        }

        //iterate thru every row in table, check if row cell values(dataset name and ID) exist in temp arrays or not
        //If condition (dataset exist, Unique ID does not exist) is fuifilled,
        //Takes the row and insert in finaleditedholder
        function addAllFieldsfromDataset() {
          fieldTable.rows().every(function () {
            if (tempdatasetnameholder.has(this.data()[2])) {
              let edited_tags = $(fieldTable.cell(this.index(), 4).node()).find("input").val().replace(/\s/g, "").split(",");
              let orginal_tags = $(fieldTable.cell(this.index(), 5).node()).find("input").val().replace(/\s/g, "").split(",");
              var edited_tags_as_Object = new Object();
              var orginal_tags_as_Object = new Object();
              for (let j = 0; j < edited_tags.length; j++) {
                edited_tags_as_Object[edited_tags[j]] = { Tag: edited_tags[j], Description: "" };
              }

              for (var edited_key of Object.keys(edited_tags_as_Object)) {
                if (Object.keys(changedTagsObjectholder).includes(edited_key)) {
                  edited_tags_as_Object[edited_key] = {
                    Tag: changedTagsObjectholder[edited_key]["Tag"],
                    Description: changedTagsObjectholder[edited_key]["Description"],
                  };
                } else if (Object.keys(allTagsObject).includes(edited_key)) {
                  edited_tags_as_Object[edited_key] = {
                    Tag: allTagsObject[edited_key]["Tag"],
                    Description: allTagsObject[edited_key]["Description"],
                  };
                }
              }
              for (let k = 0; k < orginal_tags.length; k++) {
                orginal_tags_as_Object[orginal_tags[k]] = { Tag: orginal_tags[k], Description: "" };
              }

              for (var original_key of Object.keys(orginal_tags_as_Object)) {
                if (Object.keys(changedTagsObjectholder).includes(original_key)) {
                  orginal_tags_as_Object[original_key] = {
                    Tag: changedTagsObjectholder[original_key]["Tag"],
                    Description: changedTagsObjectholder[original_key]["Description"],
                  };
                } else if (Object.keys(allTagsObject).includes(original_key)) {
                  orginal_tags_as_Object[original_key] = {
                    Tag: allTagsObject[original_key]["Tag"],
                    Description: allTagsObject[original_key]["Description"],
                  };
                }
              }

              let date = new Date();
              Object.assign(editedrowsholder, {
                ID: parseInt(this.data()[0]),
                Platform_Name: this.data()[1],
                Dataset_Name: this.data()[2],
                Field_Name: this.data()[3],
                Editable_Tags: edited_tags_as_Object,
                Original_Tags: orginal_tags_as_Object,
                Description: $(fieldTable.cell(this.index(), 6).node()).find("input").val(),
                Date_Modified: Date.parse(date.toLocaleString()),
              });
              //If row id of row with same dataset name of edited array is > current selected row, insert row from temp array before, else insert after
              finaleditedholder.push(editedrowsholder);

              editedrowsholder = {};
            }
          });
        }
        //Adds the dataset level properties to each field objects
        function addDatasetProperties() {
          datasetTable.rows().every(function () {
            for (let j = 0; j < finaleditedholder.length; j++) {
              var global_tags_as_Object = new Object();
              if (this.data()[0] === finaleditedholder[j].Platform_Name && this.data()[1] === finaleditedholder[j].Dataset_Name) {
                let global_tags = $(datasetTable.cell(this.index(), 3).node()).find("input").val().replace(/\s/g, "").split(",");
                for (let j = 0; j < global_tags.length; j++) {
                  global_tags_as_Object[global_tags[j]] = { Tag: global_tags[j], Description: "" };
                }
                for (var global_key of Object.keys(global_tags_as_Object)) {
                  if (Object.keys(changedTagsObjectholder).includes(global_key)) {
                    global_tags_as_Object[global_key] = {
                      Tag: changedTagsObjectholder[global_key]["Tag"],
                      Description: changedTagsObjectholder[global_key]["Description"],
                    };
                  } else if (Object.keys(allTagsObject).includes(global_key)) {
                    global_tags_as_Object[global_key] = {
                      Tag: allTagsObject[global_key]["Tag"],
                      Description: allTagsObject[global_key]["Description"],
                    };
                  }
                }

                Object.assign(finaleditedholder[j], {
                  Browse_Path: $(datasetTable.cell(this.index(), 2).node()).find("input").val().replace(/\s/g, "").split(","),
                  Global_Tags: global_tags_as_Object,
                  Dataset_Description: $(datasetTable.cell(this.index(), 4).node()).find("input").val(),
                  Origin: this.data()[6],
                });
              }
            }
          });
        }
        //Check for changes in the Tags table
        anyChangesfromTags();
        //Checks for changes in fields table, add dataset to tempdatasetArray
        anyChangesfromFields();
        //Checks for changes in dataset table, add dataset to tempdatasetArray if not in Array
        anyChangesfromDatasets();
        // Assigns all fields with changes or not to an object base on Tempdatasetholder and TempIdholder
        addAllFieldsfromDataset();
        //Adds dataset level properties to the fields assigned to the object
        addDatasetProperties();
        console.log("Payload to send to FASTAPI: ", finaleditedholder);
        console.log("Tags Edited: ", changedTagsObjectholder);
        if (!finaleditedholder.length) {
          axios
            .post(
              "http://localhost:8000/updatetag",

              changedTagsObjectholder,

              {
                headers: {
                  // Overwrite Axios's automatically set Content-Type
                  "Content-Type": "application/json",
                },
              }
            )
            .then((res) => {
              window.alert(res.data);
              window.location.reload();
            })
            .catch((error) => {
              window.alert("Error, Try refresh first and try again\r\n\r\nIf not " + error.response.data);
              window.location.reload(); //Logs a string: Error: Request failed with status code 404
            });
        } else {
          axios
            .post(
              "http://localhost:8000/getresult",

              finaleditedholder,

              {
                headers: {
                  // Overwrite Axios's automatically set Content-Type
                  "Content-Type": "application/json",
                },
              }
            )
            .then((res) => {
              window.alert(res.data);
              window.location.reload();
            })
            .catch((error) => {
              window.alert("Error, Try refresh first and try again\r\n\r\nIf not " + error.response.data);
              window.location.reload(); //Logs a string: Error: Request failed with status code 404
            });
        }
      });

      // this number is the timeout timer setting, IMPORTANT IF UR RECORDS TAKE LONGER, SET A LONGER TIMEOUT. If undefined, default value is 3000ms This setting is defined in .env file
    }, process.env.REACT_APP_TIMEOUT_SETTING ?? 3000);
  }

  render() {
    //Datatable HTML
    return (
      <div className="MainDiv">
        <div class="jumbotron text-center">
          <h3>Datahub Tagging UI</h3>
        </div>
        <div className="container">
          <Tabs fill defaultActiveKey="Datasets" id="uncontrolled-tab-example" className="mb-3">
            <Tab eventKey="Datasets" title="Datasets">
              <table id="datasetTable" class="table table-striped table-bordered table-sm row-border hover mb-5">
                <thead>
                  <tr>
                    {this.state.datasetcols.map((result) => {
                      return <th>{result}</th>;
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
                    );
                  })}
                </tbody>
              </table>
            </Tab>

            <Tab eventKey="Fields" title="Fields">
              <table id="fieldTable" class="table table-striped table-bordered table-sm row-border hover mb-5">
                <thead>
                  <tr>
                    {this.state.fieldcols.map((result) => {
                      return <th>{result}</th>;
                    })}
                  </tr>
                </thead>
                <tbody>
                  {this.state.fieldrows.map((result) => {
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
                    );
                  })}
                </tbody>
              </table>
            </Tab>
            <Tab eventKey="Tags" title="Tags">
              <table id="tagTable" class="table table-striped table-bordered table-sm row-border hover mb-5">
                <thead>
                  <tr>
                    {this.state.tagscols.map((result) => {
                      return <th>{result}</th>;
                    })}
                  </tr>
                </thead>
                <tbody>
                  {this.state.tagrows.map((result) => {
                    return (
                      <tr>
                        <td>{result.Tag}</td>
                        <td>{result.Description}</td>
                        <td>{result.Count}</td>
                      </tr>
                    );
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
