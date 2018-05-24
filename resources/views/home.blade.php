@extends('layouts.app')

@section('content')
<div class="container">
    <div class="row justify-content-center">
        <div class="col-md-8">
            <div class="card">
                <div class="card-header">Dashboard</div>

                <div class="card-body">
                    @if (session('status'))
                        <div class="alert alert-success">
                            {{ session('status') }}
                        </div>
                    @endif

                    You are logged in!
                </div>
                <div class="card-body">
                    <table id="example" class="display" style="width:100%">
                        <thead>
                        <tr>
                            <th>Index</th>
                            <th>Email</th>
                            <th>Sourceid</th>
                            <th>Email Domain</th>
                            <th>Status</th>
                            <th>Is Data Clean</th>
                        </tr>
                        </thead>
                        <tfoot>
                        <tr>
                            <th>First name</th>
                            <th>Last name</th>
                            <th>Position</th>
                            <th>Office</th>
                            <th>Start date</th>
                            <th>Salary</th>
                        </tr>
                        </tfoot>
                    </table>
                </div>
            </div>
        </div>
    </div>
</div>
@endsection
@section('js')
    <script>
        $(document).ready(function() {
            $.noConflict();
            $('#example').DataTable( {
                "sPaginationType": "simple",
                "lengthMenu": [10],
                "searching":false,
                "ordering": false,
                "processing": true,
                "serverSide": true,
                "ajax": "/json"
            } );
            $('#example_info').hide();
        } );
    </script>
@endsection
@section('css')

@endsection
