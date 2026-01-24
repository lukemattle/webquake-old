# WebQuake
A web application which uses live earthquake reports and live tsunami information to mitigate the short-term effects of an earthquake in Japan. This project was created for OCR A-Level Computer Science coursework.

Japan has thousands of seismological observation stations embedded in land & sea all over. When an earthquake occurs in/near Japan, these stations pick up seismic waves and when 2 or more of them observe abnormalities, the Japan Meteorological Agency (JMA) analyses and predicts an approximate location for the earthquake's intensity and epicenter. If the estimated intensity (over land) is severe enough, an emergency alert is sent to all people in the area to warn them of strong shaking. <br>If the earthquake occurred over sea (and it was strong enough, among other factors), the JMA further analyses the earthquake to determine if a tsunami is possible, and releases tsunami warnings as appropriate. WebQuake aims to collate this information into one webpage, for easy information access in an emergency, available in both English and Japanese. It also uses web notifications to deliver earthquake and tsunami warnings.

For a deeper dive, you can read the [document](https://github.com/lukemattle/webquake/blob/main/WebQuake.pdf) created for this project. While this document was created to be as informative as possible, it also had to be created to a specification, so some information may seem irrelevant. All details in the document were correct as of March 2025.<br>As per the OCR guidance around AI usage, no LLMs were used to aid the development of code or documentation for this project.

Project DM-DSS was used to source live data for this project. Access to this data requires a paid subscription, which is why this project doesn't have a live example. You can view screenshots of the webapp in the [document](https://github.com/lukemattle/webquake/blob/main/WebQuake.pdf).

## Current limitations:
- Cancelled warnings are not handled.
- Multiple simultaneous warnings (of the same type) are not displayed concurrently. WebQuake will display the warning with the most recent update.
- In tsunami & _**past**_ earthquake information, "very shallow" and "more than 700km" (for quake depth) are not translated into English.
