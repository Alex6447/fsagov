from datetime import date
from typing import Optional
from pydantic import BaseModel, Field


class ShowcaseRecord(BaseModel):
    id: int
    id_type: int = Field(..., alias="idType")
    name_type: str = Field(..., alias="nameType")
    id_status: int = Field(..., alias="idStatus")
    name_status: str = Field(..., alias="nameStatus")
    name_type_activity: Optional[str] = Field(None, alias="nameTypeActivity")
    ids_type_activity: Optional[str] = Field(None, alias="idsTypeActivity")
    reg_number: str = Field(..., alias="regNumber")
    reg_date: Optional[date] = Field(None, alias="regDate")
    full_name: str = Field(..., alias="fullName")
    address: Optional[str] = None
    federal_district: Optional[str] = Field(None, alias="federalDistrict")
    fa_country: Optional[str] = Field(None, alias="faCountry")
    fa_name: Optional[str] = Field(None, alias="faName")
    fa_name_eng: Optional[str] = Field(None, alias="faNameEng")
    solution_number: Optional[str] = Field(None, alias="solutionNumber")
    unique_register_number: Optional[str] = Field(None, alias="uniqueRegisterNumber")
    fa_id_status: Optional[int] = Field(None, alias="faIdStatus")
    has_eng_version: Optional[bool] = Field(None, alias="hasEngVersion")
    full_name_eng: Optional[str] = Field(None, alias="fullNameEng")
    short_name_eng: Optional[str] = Field(None, alias="shortNameEng")
    head_full_name_eng: Optional[str] = Field(None, alias="headFullNameEng")
    address_eng: Optional[str] = Field(None, alias="addressEng")
    applicant_full_name_eng: Optional[str] = Field(None, alias="applicantFullNameEng")
    applicant_inn: Optional[str] = Field(None, alias="applicantInn")
    applicant_full_name: Optional[str] = Field(None, alias="applicantFullName")
    oa_description: Optional[str] = Field(None, alias="oaDescription")
    oa_description_eng: Optional[str] = Field(None, alias="oaDescriptionEng")
    combined_sign_id: Optional[int] = Field(None, alias="combinedSignId")
    okved_nsi_name: Optional[str] = Field(None, alias="okvedNsiName")
    is_government_company: Optional[bool] = Field(None, alias="isGovernmentCompany")
    is_foreign_organization: Optional[bool] = Field(None, alias="isForeignOrganization")
    insert_national_part_name: Optional[str] = Field(
        None, alias="insertNationalPartName"
    )

    class Config:
        populate_by_name = True
