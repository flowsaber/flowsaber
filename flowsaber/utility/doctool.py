class DocInheritorBase(type):
    """
    Comes from https://github.com/GangCaoLab/CoolBox/blob/master/coolbox/utilities/doctool.py
    """

    def __new__(mcs, class_name, class_bases, class_dict, *args, **kwargs):
        from abc import abstractproperty
        # for class doc
        __doc__ = class_dict.get("__doc__", None)
        for p_cls in class_bases:
            p_cls_doc = p_cls.__doc__
            __doc__ = mcs.class_doc_inherit(p_cls_doc, __doc__)
        class_dict["__doc__"] = __doc__

        # for attr doc
        for attr, attribute in class_dict.items():
            if not mcs.inherit_attr(attr, attribute):
                continue
            child_attr = attribute
            # for static/class methods, the __doc__ is unchangeable
            if isinstance(child_attr, (staticmethod, classmethod)):
                child_attr = child_attr.__func__

            parent_attr_doc = None
            # Even for middle cls with no attr, the hasattr will find the attr recursively.
            for p_cls in (cls for cls in class_bases if hasattr(cls, attr)):
                parent_attr_doc = getattr(p_cls, attr).__doc__
                if parent_attr_doc is not None:
                    break

            if parent_attr_doc is None:
                continue

            doc = mcs.attr_doc_inherit(parent_attr_doc, child_attr.__doc__)
            try:
                child_attr.__doc__ = doc
            # property.__doc__ is read-only in Python 2 (TypeError), 3.3 - 3.4 (AttributeError)
            except (TypeError, AttributeError) as err:
                if type(child_attr) in (property, abstractproperty):
                    new_prop = property(
                        fget=child_attr.fget,
                        fset=child_attr.fset,
                        fdel=child_attr.fdel,
                        doc=doc,
                    )
                    if isinstance(child_attr, abstractproperty):
                        new_prop = abstractproperty(new_prop)
                    class_dict[attr] = new_prop
                else:
                    raise type(err)(err)

        return type.__new__(mcs, class_name, class_bases, class_dict)

    @classmethod
    def class_doc_inherit(mcs, p_doc, c_doc) -> str:
        return c_doc

    @classmethod
    def attr_doc_inherit(mcs, p_doc, c_doc) -> str:
        return c_doc

    @classmethod
    def inherit_attr(mcs, attr_name, attr):
        from types import FunctionType, MethodType
        # inherit docstring for method, static-method, class-method, abstract-method, decorated-method, and property
        return isinstance(attr, (FunctionType, MethodType, classmethod, staticmethod, property))


class NumpyDocInheritor(DocInheritorBase):

    @classmethod
    def build(mcs, p_doc: str, c_doc: str) -> str:
        raise NotImplementedError
        if not p_doc or not c_doc:
            return p_doc or c_doc

        p_doc = p_doc
        c_doc = c_doc

        # reuse parents' doc except for `Extended Summary`
        for section, content in c_doc.items():
            if section != "Extended Summary" and (not content and p_doc[section]):
                c_doc[section] = p_doc[section]

        # merge parameters
        c_params = [param for param in c_doc['Parameters']]
        c_param_names = {param.name for param in c_params}
        p_params = [param for param in p_doc['Parameters'] if param.name not in c_param_names]
        c_params += p_params

        c_doc['Parameters'] = c_params

        return str(c_doc)

    @classmethod
    def class_doc_inherit(mcs, p_doc, c_doc) -> str:
        return mcs.build(p_doc, c_doc)

    @classmethod
    def attr_doc_inherit(mcs, p_doc, c_doc) -> str:
        return mcs.build(p_doc, c_doc)
