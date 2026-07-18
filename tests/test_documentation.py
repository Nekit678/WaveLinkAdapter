from __future__ import annotations

import inspect
import unittest

import wavelink_adapter


class PublicApiDocumentationTests(unittest.TestCase):
    """Protect IDE documentation for the installed public API."""

    def test_exported_classes_and_functions_have_docstrings(self) -> None:
        missing: list[str] = []

        for name in wavelink_adapter.__all__:
            value = getattr(wavelink_adapter, name)
            if not (inspect.isclass(value) or inspect.isfunction(value)):
                continue
            if not inspect.getdoc(value):
                missing.append(name)

        self.assertEqual(missing, [], f"missing public docstrings: {missing}")

    def test_public_methods_defined_by_exported_classes_have_docstrings(self) -> None:
        missing: list[str] = []

        for class_name in wavelink_adapter.__all__:
            value = getattr(wavelink_adapter, class_name)
            if not inspect.isclass(value):
                continue
            if not value.__module__.startswith("wavelink_adapter"):
                continue

            for method_name, descriptor in vars(value).items():
                if method_name.startswith("_"):
                    continue
                if isinstance(descriptor, (classmethod, staticmethod)):
                    target = descriptor.__func__
                elif isinstance(descriptor, property):
                    target = descriptor.fget
                elif inspect.isfunction(descriptor):
                    target = descriptor
                else:
                    continue

                if target is not None and not inspect.getdoc(target):
                    missing.append(f"{class_name}.{method_name}")

        self.assertEqual(missing, [], f"missing method docstrings: {missing}")


if __name__ == "__main__":
    unittest.main()
