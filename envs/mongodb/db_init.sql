use admin
db.createUser(
  {
    user: "SPECDataAdmin",
    pwd: passwordPrompt(), //
    roles: [
      { role: "userAdminAnyDatabase", db: "admin" },
      { role: "readWriteAnyDatabase", db: "admin" }
    ]
  }
)

use ODS_SPEC_OSG_CPU2017
db.createUser(
  {
    user: "ODS_SPEC_OSG_CPU2017_Root",
    pwd: passwordPrompt(), 
    roles: [
       { role: "readWrite", db: "ODS_SPEC_OSG_CPU2017" }
    ]
  }
)

use ODS_SPEC_OSG_CPU2017
db.createUser(
  {
    user: "ODS_SPEC_OSG_CPU2017_Reader",
    pwd: passwordPrompt(), 
    roles: [
       { role: "read", db: "ODS_SPEC_OSG_CPU2017" }
    ]
  }
)
