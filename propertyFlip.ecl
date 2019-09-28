Layout := RECORD
    STRING number_of_rooms;
    STRING assessment_date;
    STRING beginning_point;
    STRING book_and_page;
    STRING building_code;
    STRING building_code_description;
    STRING category_code;
    STRING category_code_description;
    STRING census_tract;
    STRING central_air;
    STRING cross_reference;
    STRING date_exterior_condition;
    STRING depth;
    STRING exempt_building;
    STRING exempt_land;
    STRING exterior_condition;
    STRING fireplaces;
    STRING frontage;
    STRING fuel;
    STRING garage_spaces;
    STRING garage_type;
    STRING general_construction;
    STRING geographic_ward;
    STRING homestead_exemption;
    STRING house_extension;
    STRING house_number;
    STRING interior_condition;
    STRING location;
    STRING mailing_address_1;
    STRING mailing_address_2;
    STRING mailing_care_of;
    STRING mailing_city_state;
    STRING mailing_street;
    STRING mailing_zip;
    STRING market_value;
    STRING market_value_date;
    STRING number_of_bathrooms;
    STRING number_of_bedrooms;
    STRING basements;
    STRING number_stories;
    STRING off_street_open;
    STRING other_building;
    STRING owner_1;
    STRING owner_2;
    STRING parcel_number;
    STRING parcel_shape;
    STRING quality_grade;
    STRING recording_date;
    STRING registry_number;
    STRING sale_date;
    STRING sale_price;
    STRING separate_utilities;
    STRING sewer;
    STRING site_type;
    STRING state_code;
    STRING street_code;
    STRING street_designation;
    STRING street_direction;
    STRING street_name;
    STRING suffix;
    STRING taxable_building;
    STRING taxable_land;
    STRING topography;
    STRING total_area;
    STRING total_livable_area;
    STRING type_heater;
    STRING unfinished;
    STRING unit;
    STRING utility;
    STRING view_type;
    STRING year_built;
    STRING year_built_estimate;
    STRING zip_code;
    STRING zoning;
    STRING objectid;
    STRING lat;
    STRING lng;
END;

filteredlayout := RECORD
  STRING category_code;
  STRING exterior_condition;
  STRING interior_condition;
  STRING number_of_bathrooms;
  STRING number_of_bedrooms;
  STRING year_built;
  STRING market_value;
  STRING parcel_number;
  STRING central_air;
  STRING  zip_code;
END;

filteredlayout1 := RECORD
  INTEGER category_code;
  INTEGER exterior_condition;
  INTEGER interior_condition;
  INTEGER number_of_bathrooms;
  INTEGER number_of_bedrooms;
  INTEGER year_built;
  INTEGER market_value;
  INTEGER parcel_number;
  String central_air;
  INTEGER zip_code;
END;

ourDs := DATASET('~ksu::hackathon::opa_properties_public.csv', Layout, CSV(HEADING(1)));
//OUTPUT(ourDs, NAMED('Raw'));

getAirPoint(string airCond) := function //function definition for central_air point evaluation
  return CASE(airCond, 
              'Y' => 6,
              'N' => 1,
      				3);
end;

ourDs1 := project(ourDs, transform(filteredlayout1, self.category_code := (integer)left.category_code, //changes raw data to integers
                                  self.exterior_condition := (integer)left.exterior_condition,
                                  self.interior_condition := (integer)left.interior_condition,
                                  self.number_of_bathrooms := (integer)left.number_of_bathrooms,
                                  self.central_air := left.central_air,
                                  self.number_of_bedrooms := (integer)left.number_of_bedrooms,
                  								self.year_built := (integer)left.year_built,
                  								self.market_value := (integer)left.market_value,
                  								self.parcel_number := (integer)left.parcel_number,
                                  self.zip_code :=(integer)left.zip_code,
                                  self := left 
                                  ));

//Cleaning data: Applies constraints to get desired results
properties := ourDs1(category_code = 1 );
properties1 := properties(exterior_condition in [4,5,6] OR interior_condition in [4,5,6,7]);
properties2 := properties1(number_of_bedrooms<> 0);
output(properties2[1..1000]);

//Point system for the Exterior Condition
getExtPoint(integer extCond) := function
  return CASE(extCond, 
              7 => 1,
              6 => 3,
              5 => 5,
              4 => 5,
              3 => 7,
              2 => 9,
              1 => 10,
              0);
end;

//Point system for the Interior condition
getIntPoint(integer intCond) := function
  return CASE(intCond, 
              7 => 1,
              6 => 3,
              5 => 5,
              4 => 5,
              3 => 7,
              2 => 9,
              1 => 10,
              0);
end;

//Point system for the Year condition
getYearPoint(integer year) := function
  return CASE(ROUND(((2019-year)/10)),
  						12 => 1,	  //oldest houses (1900s)
  						11 => 1,
  						10 => 1,
  						9 => 2,
  						8 => 2, 
  						7 => 3,
  						6 => 3,
  						5 => 4,
  						4 => 4,     //1977 houses
  						3 => 5,
  						2 => 5,
  						1 => 6,
  						0 => 8,    //current houses (2010s+)
  						5);  						
end; 

newLayout := RECORD
    integer flip_potential := 0;
    integer extCond;
    integer intCond;
    integer year;
    integer airCond;
end;

newDS := PROJECT(properties2, TRANSFORM(newLayout, 
                                                     SELF.extCond := getExtPoint(LEFT.exterior_condition),
                                                     SELF.intCond := getIntPoint(LEFT.interior_condition),
                                                     SELF.year:= getYearpoint(LEFT.year_built),
                                                     SELF.airCond := getAirPoint(LEFT.central_air),
                                                     SELF := LEFT));

result := PROJECT(newDS, TRANSFORM(RECORDOF(LEFT), SELF.flip_potential := (LEFT.extCond + 
                                                                          LEFT.intCond +
                                                                          LEFT.year +
                                                                          LEFT.airCond)/4,
                                   									SELF := LEFT));
OUTPUT(result[1..100], NAMED('result'));

