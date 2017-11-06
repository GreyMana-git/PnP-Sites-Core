using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SharePoint.Client;
using System.Reflection;
using System.Text.RegularExpressions;
using Microsoft.SharePoint.Client.Taxonomy;
using System.Globalization;

namespace OfficeDevPnP.Core.Utilities
{
    class PortableListItem
    {
        public static readonly object NeedsQuery = new object();

        public static readonly object SkipField = new object();

        private static readonly Dictionary<string, MethodInfo> getters = new Dictionary<string, MethodInfo>();

        private static readonly Dictionary<string, MethodInfo> setters = new Dictionary<string, MethodInfo>();

        private ListItem item;

        private ClientRuntimeContext context;

        private Dictionary<string, Field> fields;

        private Dictionary<string, string> contentTypes;

        static PortableListItem()
        {
            // Self-Discover getters and setters
            foreach (MethodInfo method in typeof(PortableListItem).GetMethods(BindingFlags.Instance | BindingFlags.NonPublic))
            {
                // Getters
                foreach (GetterAttribute attribute in method.GetCustomAttributes<GetterAttribute>())
                {
                    PortableListItem.getters.Add((attribute.Field != null ? $"{attribute.Type}.{attribute.Field}" : attribute.Type), method);
                }

                // Setters
                foreach (SetterAttribute attribute in method.GetCustomAttributes<SetterAttribute>())
                {
                    PortableListItem.setters.Add((attribute.Field != null ? $"{attribute.Type}.{attribute.Field}" : attribute.Type), method);
                }
            }
        }

        public PortableListItem(ListItem item)
        {
            this.item = item;
            this.context = this.item.Context;
        }

        public void Initialize()
        {
            List list = this.item.ParentList;

            bool needsQuery = false;
            bool available = true;

            string[] properties = new string[] { "Id", "Hidden", "InternalName", "TypeAsString", "DefaultValue" };
            available = available && list.IsObjectPropertyInstantiated("Fields");
            available = available && list.Fields.All(field => properties.All(property => field.IsPropertyAvailable(property)));
            if (!available)
            {
                context.Load(list.Fields, collection => collection.Include(f => f.Id, f => f.Hidden, f => f.InternalName, f => f.TypeAsString, f => f.DefaultValue));
                needsQuery = true;
            }

            properties = new string[] { "Id", "Name" };
            available = list.IsObjectPropertyInstantiated("ContentTypes");
            available = available && list.ContentTypes.All(ct => properties.All(property => ct.IsPropertyAvailable(property)) || ct.IsObjectPropertyInstantiated("Parent"));
            if (!available)
            {
                context.Load(list.ContentTypes, collection => collection.Include(ct => ct.StringId, ct => ct.Name, ct => ct.Parent));
                needsQuery = true;
            }

            if (needsQuery)
            {
                Console.WriteLine("Init: Executing query in initialize...");
                context.ExecuteQuery();
            }

            // Add fields by name and by lowercased id
            this.fields = new Dictionary<string, Field>();
            foreach (Field field in list.Fields)
            {
                this.fields.Add(field.InternalName, field);
                this.fields.Add(field.Id.ToString().ToLowerInvariant(), field);
            }

            // Add content types by name and lowercased id
            this.contentTypes = new Dictionary<string, string>();
            foreach (ContentType ct in list.ContentTypes)
            {
                this.contentTypes.Add(ct.Parent.StringId.ToLowerInvariant(), ct.Parent.StringId);
                this.contentTypes.Add(ct.StringId.ToLowerInvariant(), ct.Parent.StringId);
                this.contentTypes.Add(ct.Name.ToLowerInvariant(), ct.Parent.StringId);
            }

            // Secondary lookups for more informations
            needsQuery = false;
            foreach (Field field in list.Fields)
            {
                if (field is FieldLookup && !field.IsPropertyAvailable("AllowMultipleValues"))
                {
                    this.context.Load((FieldLookup)field, f => f.AllowMultipleValues);
                    needsQuery = true;
                }

                if (field is FieldChoice && !field.IsObjectPropertyInstantiated("Choices"))
                {
                    this.context.Load((FieldChoice)field, f => f.Choices);
                    needsQuery = true;
                }
            }

            if (needsQuery)
            {
                Console.WriteLine("Init: Secondary query needed for resolving the multiple value info");
                this.context.ExecuteQuery();
            }
        }

        enum Direction
        {
            Getter,
            Setter
        }

        private FieldProcessor GetFieldProcessor(string fieldname, Direction direction, object value)
        {
            Dictionary<string, MethodInfo> processors = (direction == Direction.Getter ? PortableListItem.getters : PortableListItem.setters);

            Field field;
            if (fields.TryGetValue(fieldname, out field))
            {
                MethodInfo method;
                if (processors.TryGetValue(string.Format("{0}.{1}", field.TypeAsString, field.InternalName), out method) || processors.TryGetValue(field.TypeAsString, out method))
                {
                    object result = method.Invoke(this, new object[] { field, direction, value });
                    if (result is IEnumerable<object> && !(result is List<object> || result is object[] || result is Dictionary<object, object>))
                    {
                        return new FieldProcessor(field, null, ((IEnumerable<object>)result).GetEnumerator());
                    }

                    return new FieldProcessor(field, result, null);
                }

                Console.WriteLine(string.Format("Missing info for {0}, Type: {1}", field.InternalName, field.TypeAsString));
                return new FieldProcessor(field, PortableListItem.SkipField, null);
                throw new ArgumentException(string.Format("No processor registered for field type {0}", field.TypeAsString));
            }

            throw new ArgumentException(string.Format("Unable to find field {0} in list.", fieldname));
        }

        private void ExecuteProcessors(IEnumerable<FieldProcessor> processors)
        {
            bool needsQuery = false;
            do
            {
                needsQuery = false;
                foreach (FieldProcessor processor in processors)
                {
                    if (processor.Processor != null && processor.Processor.MoveNext())
                    {
                        processor.Value = processor.Processor.Current;
                        needsQuery = needsQuery || processor.Value == PortableListItem.NeedsQuery;
                    }
                }

                if (needsQuery)
                {
                    Console.WriteLine("Executing query for item values...");
                    this.context.ExecuteQueryRetry();
                }
            } while (needsQuery);

            return;
        }

        public void Set(Dictionary<string, object> values)
        {
            this.Initialize();
            List<FieldProcessor> processors = new List<FieldProcessor>();

            // Initialize content type field first
            string ctfieldname = values.ContainsKey("ContentTypeId") ? "ContentTypeId" : values.Keys.Where(k => k.ToLowerInvariant() == "contenttype").FirstOrDefault();
            if (!string.IsNullOrEmpty(ctfieldname))
            {
                processors.Add(this.GetFieldProcessor("ContentTypeId", Direction.Setter, values[ctfieldname]));
            }

            // Create all field processors
            foreach (string fieldname in values.Keys)
            {
                if (fieldname != "ContentTypeId" && fieldname.ToLowerInvariant() != "contenttype" && values.ContainsKey(fieldname))
                {
                    processors.Add(this.GetFieldProcessor(fieldname, Direction.Setter, values[fieldname]));
                    continue;
                }
            }

            // Execute remaining processors that may need queries
            this.ExecuteProcessors(processors);

            // Update all item fields
            foreach (FieldProcessor processor in processors)
            {
                if (processor.Value == PortableListItem.SkipField)
                {
                    continue;
                }

                string fieldname = processor.Field.InternalName;
                // Taxonomy field need special execution
                if (processor.Field is TaxonomyField)
                {
                    TaxonomyField taxField = (TaxonomyField)processor.Field;
                    if (processor.Value is TaxonomyFieldValue)
                    {
                        taxField.SetFieldValueByValue(item, (TaxonomyFieldValue)processor.Value);
                    }
                    else if (processor.Value is TaxonomyFieldValueCollection)
                    {
                        taxField.SetFieldValueByValueCollection(this.item, (TaxonomyFieldValueCollection)processor.Value);
                    }
                    else if (processor.Value == null)
                    {
                        this.item[fieldname] = processor.Value;
                    }
                }
                else
                {
                    this.item[fieldname] = processor.Value;
                }
            }

            // Done
            this.item.Update();
            this.context.ExecuteQuery();
        }

        public Dictionary<string, object> Get(IEnumerable<string> fieldnames)
        {
            this.Initialize();
            List<FieldProcessor> processors = new List<FieldProcessor>();

            if (fieldnames == null)
            {
                fieldnames = this.item.ParentList.Fields.Where(f => f.InternalName != "ContentType" && !f.Hidden).Select(f => f.InternalName).ToList();
            }

            // Check if all required fields are loaded
            bool needsQuery = false;
            foreach (string fieldname in fieldnames)
            {
                if (!item.FieldValues.ContainsKey(fieldname))
                {
                    this.context.Load(this.item, i => i[fieldname]);
                    needsQuery = true;
                }
            }

            if (needsQuery)
            {
                System.Console.WriteLine("Get Method - Need more list item values");
                this.context.ExecuteQuery();
            }

            if (this.item.IsObjectPropertyInstantiated("FieldValues"))
            {
                this.context.Load(this.item);
            }

            // Create all field processors
            foreach (string fieldname in fieldnames)
            {
                if (this.item.FieldValues.ContainsKey(fieldname))
                {
                    processors.Add(this.GetFieldProcessor(fieldname, Direction.Getter, this.item.FieldValues[fieldname]));
                }
            }

            // Execute remaining processors that may need queries
            this.ExecuteProcessors(processors);

            // Build output dictionary
            Dictionary<string, object> result = new Dictionary<string, object>();
            foreach (FieldProcessor processor in processors)
            {
                if (processor.Value == PortableListItem.SkipField)
                {
                    continue;
                }

                result.Add(processor.Field.InternalName, processor.Value);
            }

            return result;
        }

        ///////////////////////////////////////////////////////////////////////
        // Field Getter / Setter
        ///////////////////////////////////////////////////////////////////////

        [Getter("ContentTypeId")]
        private string GetContentTypeId(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is ContentType)
            {
                return this.contentTypes[((ContentType)value).StringId.ToLowerInvariant()];
            }

            if (value is string)
            {
                return this.contentTypes[(string)value];
            }

            throw new ArgumentException($"Unsupported type {value.GetType().FullName} for content type field {field.InternalName}");
        }

        [Setter("ContentTypeId")]
        private string SetContentTypeId(Field field, Direction direction, object value)
        {
            if (value is ContentType)
            {
                return ((ContentType)value).StringId;
            }

            if (value is string)
            {
                string ctid = (string)value;
                // Remove placeholder with name only (the name is also in the dictionary)
                Match match = Regex.Match(ctid, "^{contenttypeid:(?<Name>[^}]+)}$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
                if (match != null && match.Success)
                {
                    ctid = match.Groups["Name"].Value;
                }

                ctid = ctid.ToLowerInvariant();
                if (this.contentTypes.ContainsKey(ctid))
                {
                    this.item["ContentTypeId"] = this.contentTypes[ctid];
                }
            }

            throw new ArgumentException($"Unsupported type {value.GetType().FullName} for content type field {field.InternalName}");
        }

        [Getter("Text"), Setter("Text"), Getter("Note"), Setter("Note"), Getter("Computed")]
        private string ConvertTextValue(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                if (direction == Direction.Getter)
                {
                    return string.IsNullOrEmpty(field.DefaultValue) ? null : field.DefaultValue;
                }
                else
                {
                    return null;
                }
            }

            if (value is IConvertible)
            {
                return ((IConvertible)value).ToString(CultureInfo.InvariantCulture);
            }

            throw new ArgumentException($"Unsupported type {value.GetType().FullName} for text field {field.InternalName}");
        }

        [Getter("Integer"), Setter("Integer"), Getter("Counter")]
        private int ConvertIntegerValue(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                if (direction == Direction.Getter)
                {
                    return string.IsNullOrEmpty(field.DefaultValue) ? 0 : int.Parse(field.DefaultValue);
                }
                else
                {
                    return 0;
                }
            }

            if (value is IConvertible)
            {
                return ((IConvertible)value).ToInt32(CultureInfo.InvariantCulture);
            }

            throw new ArgumentException("Unsupported type for integer field: {0}", value.GetType().FullName);
        }

        [Getter("Number"), Setter("Number")]
        private double ConvertNumberValue(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                if (direction == Direction.Getter)
                {
                    return string.IsNullOrEmpty(field.DefaultValue) ? 0 : double.Parse(field.DefaultValue);
                }
                else
                {
                    return 0.0;
                }
            }

            if (value is IConvertible)
            {
                return ((IConvertible)value).ToDouble(CultureInfo.InvariantCulture);
            }

            throw new ArgumentException("Unsupported type for number field: {0}", value.GetType().FullName);
        }

        [Getter("Boolean"), Setter("Boolean")]
        private bool ConvertBooleanValue(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                if (direction == Direction.Getter)
                {
                    return string.IsNullOrEmpty(field.DefaultValue) ? false : bool.Parse(field.DefaultValue);
                }
                else
                {
                    return false;
                }
            }

            if (value is IConvertible)
            {
                return ((IConvertible)value).ToBoolean(CultureInfo.InvariantCulture);
            }

            throw new ArgumentException("Unsupported type for boolean field: {0}", value.GetType().FullName);
        }

        private List<Guid> ExtractTaxonomyGuid(string value)
        {
            List<Guid> results = new List<Guid>();
            if (string.IsNullOrEmpty(value))
            {
                return results;
            }

            string[] parts = value.Split(new string[] { ";#" }, StringSplitOptions.RemoveEmptyEntries);
            for (int index = 1; index < parts.Length; index += 2)
            {
                if (parts[index].IndexOf('|') >= 0)
                {
                    results.Add(System.Guid.Parse(parts[index].Substring(parts[index].IndexOf('|') + 1)));
                }
            }

            return results;
        }

        [Getter("TaxonomyFieldType"), Getter("TaxonomyFieldTypeMulti")]
        private object GetTaxonomyFieldTypeValue(Field field, Direction direction, object value)
        {
            TaxonomyField taxField = this.context.CastTo<TaxonomyField>(field);
            List<Guid> values;
            if (value == null)
            {
                values = this.ExtractTaxonomyGuid(taxField.DefaultValue);
                if (taxField.AllowMultipleValues)
                {
                    return values.Select(v => v.ToString("D")).ToList();
                }

                return values.Count == 0 ? null : values[0].ToString("D");
            }

            values = new List<Guid>();
            if (value is TaxonomyFieldValue)
            {
                values.Add(Guid.Parse(((TaxonomyFieldValue)value).TermGuid));
            }
            else if (value is TaxonomyFieldValueCollection)
            {
                foreach (TaxonomyFieldValue taxValue in (TaxonomyFieldValueCollection)value)
                {
                    values.Add(Guid.Parse(taxValue.TermGuid));
                }
            }

            if (taxField.AllowMultipleValues)
            {
                return values.Select(v => v.ToString("D")).ToList();
            }

            return values.Count > 0 ? values[0].ToString("D") : null;
        }

        [Setter("TaxonomyFieldType"), Setter("TaxonomyFieldTypeMulti")]
        private IEnumerable<object> SetTaxonomyFieldTypeValue(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                value = string.Empty;
            }

            TaxonomyField taxField = this.context.CastTo<TaxonomyField>(field);
            if (value is string)
            {
                if (taxField.AllowMultipleValues)
                {
                    TaxonomyFieldValueCollection values = taxField.GetFieldValueAsTaxonomyFieldValueCollection((string)value);
                    this.context.Load(values);
                    yield return PortableListItem.NeedsQuery;

                    yield return values;
                    yield break;
                }
                else
                {
                    ClientResult<TaxonomyFieldValue> fieldValue = taxField.GetFieldValueAsTaxonomyFieldValue((string)value);
                    yield return PortableListItem.NeedsQuery;

                    yield return fieldValue.Value;
                    yield break;
                }
            }
        }

        [Getter("User"), Getter("UserMulti")]
        private IEnumerable<object> GetUser(Field field, Direction direction, object value)
        {
            FieldUser userField = this.context.CastTo<FieldUser>(field);
            if (value == null)
            {
                yield return userField.AllowMultipleValues ? new List<string>() : null;
                yield break;
            }

            Web web = this.item.ParentList.ParentWeb;
            List<string> values = new List<string>();
            if (value is FieldUserValue)
            {
                User user = web.GetUserById(((FieldUserValue)value).LookupId);
                this.context.Load(user);
                yield return PortableListItem.NeedsQuery;
                values.Add(user.LoginName);
            }
            else if (value is IEnumerable<FieldUserValue>)
            {
                List<User> users = new List<User>();
                foreach (FieldUserValue uservalue in (IEnumerable<FieldUserValue>)value)
                {
                    User user = web.GetUserById(uservalue.LookupId);
                    this.context.Load(user);
                }

                yield return PortableListItem.NeedsQuery;
                values.AddRange(users.Select(u => u.LoginName));
            }
            else
            {
                throw new ArgumentException("Unsupported type for user field: {0}", value.GetType().FullName);
            }

            if (userField.AllowMultipleValues)
            {
                yield return values.ToArray();
            }
            else
            {
                yield return values.Count > 0 ? values[0] : null;
            }
        }

        [Setter("User"), Setter("UserMulti")]
        private IEnumerable<object> SetUser(Field field, Direction direction, object value)
        {
            FieldUser userField = this.context.CastTo<FieldUser>(field);
            if (value == null)
            {
                yield return userField.AllowMultipleValues ? new List<FieldUserValue>() : null;
                yield break;
            }

            Web web = this.item.ParentList.ParentWeb;
            List<FieldUserValue> values = new List<FieldUserValue>();
            if (value is string)
            {
                List<User> users = new List<User>();
                foreach (string username in ((string)value).Split(',').Select(u => u.Trim()))
                {
                    User user = web.EnsureUser(username);
                    this.context.Load(user);
                    users.Add(user);
                }

                yield return PortableListItem.NeedsQuery;
                users.ForEach(u => values.Add(new FieldUserValue() { LookupId = u.Id }));
            }
            else if (value is User)
            {
                User user = (User)value;
                if (!user.IsPropertyAvailable("Id"))
                {
                    this.context.Load(user);
                    yield return PortableListItem.NeedsQuery;
                }

                values.Add(new FieldUserValue() { LookupId = user.Id });
            }
            else if (value is IEnumerable<User>)
            {
                IEnumerable<User> users = (IEnumerable<User>)value;
                bool needsQuery = false;
                foreach (User user in (IEnumerable<User>)value)
                {
                    if (!user.IsPropertyAvailable("Id"))
                    {
                        needsQuery = true;
                        this.context.Load(user);
                    }
                }

                if (needsQuery)
                {
                    yield return PortableListItem.NeedsQuery;
                }

                values.AddRange(users.Select(u => new FieldUserValue() { LookupId = u.Id }));
            }
            else if (value is FieldUserValue)
            {
                values.Add((FieldUserValue)value);
            }
            else if (value is IEnumerable<FieldUserValue>)
            {
                foreach (FieldUserValue user in (IEnumerable<FieldUserValue>)value)
                {
                    values.Add(user);
                }
            }
            else
            {
                throw new ArgumentException("Unsupported type for user field: {0}", value.GetType().FullName);
            }

            if (userField.AllowMultipleValues)
            {
                yield return values.ToArray();
            }
            else
            {
                yield return values.Count > 0 ? values[0] : null;
            }
        }


        [Getter("DateTime")]
        string GetDateTime(Field field, Direction direction, object value)
        {
            DateTime date;
            if (value == null)
            {
                if (string.IsNullOrEmpty(field.DefaultValue))
                {
                    return null;
                }

                if (!DateTime.TryParse(field.DefaultValue, out date))
                {
                    if (field.DefaultValue.ToLowerInvariant() == "[today]")
                    {
                        date = DateTime.Now;
                        date = new DateTime(date.Year, date.Month, date.Day);
                    }
                    else
                    {
                        // Unsupported default value formula
                        return null;
                    }
                }
            }
            else if (value is DateTime)
            {
                date = (DateTime)value;
            }
            else if (value is string)
            {
                date = DateTime.Parse((string)value, CultureInfo.InvariantCulture);
            }
            else
            {
                throw new ArgumentException("Unsupported type for datetime field: {0}", value.GetType().FullName);
            }

            return date.ToString("yyyy-MM-dd'T'HH:mm:ssK", CultureInfo.InvariantCulture);
        }

        [Setter("DateTime")]
        object SetDateTime(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                return null;
            }

            if (value is DateTime)
            {
                return value;
            }

            if (value is string)
            {
                DateTime date;
                if (DateTime.TryParse((string)value, CultureInfo.InvariantCulture, DateTimeStyles.None, out date))
                {
                    return date;
                }

                throw new ArgumentException(string.Format("Unsupported string format for datetime field: {0}. Format: {1}", value.GetType().FullName, value.ToString()));
            }

            throw new ArgumentException("Unsupported type for datetime field: {0}", value.GetType().FullName);
        }

        [Getter("Choice"), Getter("MultiChoice")]
        object GetChoiceValue(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                if (string.IsNullOrEmpty(field.DefaultValue))
                {
                    return null;
                }

                value = field.DefaultValue;
            }

            if (field.TypeAsString == "MultiChoice")
            {
                if (value is string)
                {
                    return new string[] { (string)value };
                }
                else if (value is string[])
                {
                    return (string[])value;
                }
            }
            else
            {
                if (value is string)
                {
                    return (string)value;
                }
                else if (value is string[])
                {
                    string[] values = (string[])value;
                    return values.Length > 0 ? values[0] : null;
                }
            }

            throw new ArgumentException("Unsupported type for choice field: {0}", value.GetType().FullName);
        }

        [Setter("Choice"), Setter("MultiChoice")]
        object SetChoiceField(Field field, Direction direction, object value)
        {
            if (value == null)
            {
                if (field.TypeAsString == "MultiChoice")
                {
                    return new string[0];
                }
                else
                {
                    return null;
                }
            }

            if (field.TypeAsString == "MultiChoice")
            {
                if (value is string)
                {
                    return (string)value;
                }
                else if (value is string[])
                {
                    return string.Join(";#", (string[])value);
                }
            }
            else
            {
                if (value is string)
                {
                    return (string)value;
                }
                else if (value is string[])
                {
                    string[] values = (string[])value;
                    return values.Length > 0 ? values[0] : null;
                }
            }

            throw new ArgumentException("Unsupported type for choice field: {0}", value.GetType().FullName);
        }

        [Getter("Geolocation")]
        private object GetGeolocation(Field field, Direction direction, object value)
        {
            FieldGeolocation geoField = this.context.CastTo<FieldGeolocation>(field);
            return value;
        }


        private class FieldProcessor
        {
            public Field Field { get; private set; }

            public IEnumerator<object> Processor { get; private set; }

            public object Value { get; set; }

            public FieldProcessor(Field field, object value, IEnumerator<object> processor)
            {
                this.Field = field;
                this.Processor = processor;
                this.Value = value;
            }
        }


        // "Geolocation"

        /// <summary>
        /// Helper class for tagging methods of this class as a setter for a specific field type. The discovery of the setter will be done via reflection
        /// </summary>
        [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
        private class SetterAttribute : System.Attribute
        {
            public SetterAttribute(string typename, string fieldname)
            {
                this.Type = typename;
                this.Field = fieldname;
            }

            public SetterAttribute(string typename)
            {
                this.Type = typename;
                this.Field = null;
            }

            public string Type { get; private set; }

            public string Field { get; private set; }
        }

        [AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
        private class GetterAttribute : System.Attribute
        {
            public GetterAttribute(string typename, string fieldname)
            {
                this.Type = typename;
                this.Field = fieldname;
            }

            public GetterAttribute(string typename)
            {
                this.Type = typename;
                this.Field = null;
            }

            public string Type { get; private set; }

            public string Field { get; private set; }
        }

    }
}
